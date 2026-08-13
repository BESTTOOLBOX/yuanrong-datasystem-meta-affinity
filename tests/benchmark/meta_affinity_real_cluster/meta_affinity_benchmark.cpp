/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/object_client.h"

namespace {

using Clock = std::chrono::steady_clock;
using datasystem::Buffer;
using datasystem::ConnectOptions;
using datasystem::CreateParam;
using datasystem::ObjectClient;
using datasystem::ObjMetaInfo;
using datasystem::Optional;
using datasystem::Status;

constexpr size_t MAX_GET_OBJ_META_INFO_KEYS = 64;

struct Arguments {
    std::string task;
    std::string worker;
    std::string runId;
    std::string scenario;
    std::string buildLabel;
    std::string clientId;
    std::string comparisonId;
    std::string topologyId;
    std::string keySetDigest;
    std::string output;
    std::string keysFile;
    std::string expectedLocation;
    uint64_t payloadBytes = 0;
    int repeat = 0;
    int warmup = 0;
    int iterations = 0;
    int requestTimeoutMs = 60000;
    int locationTimeoutMs = 120000;
    int postWriteWaitMs = 0;
    int64_t startEpochMs = 0;
    int intervalMs = 0;
};

struct OperationSample {
    int iteration;
    bool warmup;
    int64_t latencyNs;
    Status status;
    int64_t completedEpochMs;
};

std::string JsonEscape(const std::string &value)
{
    std::ostringstream out;
    for (unsigned char ch : value) {
        switch (ch) {
            case '"':
                out << "\\\"";
                break;
            case '\\':
                out << "\\\\";
                break;
            case '\b':
                out << "\\b";
                break;
            case '\f':
                out << "\\f";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\r':
                out << "\\r";
                break;
            case '\t':
                out << "\\t";
                break;
            default:
                if (ch < 0x20) {
                    const char hex[] = "0123456789abcdef";
                    out << "\\u00" << hex[ch >> 4] << hex[ch & 0xf];
                } else {
                    out << ch;
                }
        }
    }
    return out.str();
}

bool IsSafeIdentifier(const std::string &value)
{
    if (value.empty() || value.size() > 128) {
        return false;
    }
    return std::all_of(value.begin(), value.end(), [](unsigned char ch) {
        return std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.';
    });
}

bool ParseInteger(const std::string &name, const std::string &value, int64_t min, int64_t max, int64_t &result)
{
    try {
        size_t parsed = 0;
        const long long number = std::stoll(value, &parsed);
        if (parsed != value.size() || number < min || number > max) {
            std::cerr << "Invalid --" << name << ": " << value << std::endl;
            return false;
        }
        result = number;
        return true;
    } catch (const std::exception &) {
        std::cerr << "Invalid --" << name << ": " << value << std::endl;
        return false;
    }
}

void PrintUsage(const char *program)
{
    std::cerr
        << "Usage: " << program << " --task writer|reader|inspect|cleanup"
        << " --worker HOST:PORT --run-id ID --scenario NAME --build-label LABEL --client-id ID"
        << " --payload-bytes N --repeat N --warmup N --iterations N --output FILE [options]\n"
        << "Options:\n"
        << "  --keys-file FILE              Optional exact key manifest shared by all roles\n"
        << "  --comparison-id ID            B0/PR comparison identifier copied to every result\n"
        << "  --topology-id ID              Physical topology identifier copied to every result\n"
        << "  --expected-location WORKER_ID Writer waits until every key has this data location\n"
        << "  --location-timeout-ms N       Location wait timeout (default 120000)\n"
        << "  --post-write-wait-ms N        Untimed wait after Put and global-ref setup\n"
        << "  --request-timeout-ms N        SDK request timeout (default 60000)\n"
        << "  --start-epoch-ms N            Reader wall-clock start barrier\n"
        << "  --interval-ms N               Reader per-key alignment interval\n";
}

bool ParseArguments(int argc, char **argv, Arguments &args)
{
    std::map<std::string, std::string> values;
    for (int i = 1; i < argc; ++i) {
        std::string token(argv[i]);
        if (token.rfind("--", 0) != 0) {
            std::cerr << "Unexpected argument: " << token << std::endl;
            return false;
        }
        token.erase(0, 2);
        const auto equals = token.find('=');
        if (equals != std::string::npos) {
            values[token.substr(0, equals)] = token.substr(equals + 1);
        } else {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for --" << token << std::endl;
                return false;
            }
            values[token] = argv[++i];
        }
    }

    const auto readString = [&](const std::string &name, std::string &destination, bool required) {
        auto it = values.find(name);
        if (it == values.end()) {
            if (required) {
                std::cerr << "Missing --" << name << std::endl;
                return false;
            }
            return true;
        }
        destination = it->second;
        return true;
    };
    if (!readString("task", args.task, true) || !readString("worker", args.worker, true)
        || !readString("run-id", args.runId, true) || !readString("scenario", args.scenario, true)
        || !readString("build-label", args.buildLabel, true) || !readString("client-id", args.clientId, true)
        || !readString("output", args.output, true) || !readString("keys-file", args.keysFile, false)
        || !readString("comparison-id", args.comparisonId, false)
        || !readString("topology-id", args.topologyId, false)
        || !readString("expected-location", args.expectedLocation, false)) {
        return false;
    }

    const auto readInteger = [&](const std::string &name, int64_t min, int64_t max, int64_t &destination,
                                 bool required) {
        auto it = values.find(name);
        if (it == values.end()) {
            if (required) {
                std::cerr << "Missing --" << name << std::endl;
                return false;
            }
            return true;
        }
        return ParseInteger(name, it->second, min, max, destination);
    };

    int64_t payload = 0;
    int64_t repeat = args.repeat;
    int64_t warmup = args.warmup;
    int64_t iterations = args.iterations;
    int64_t requestTimeout = args.requestTimeoutMs;
    int64_t locationTimeout = args.locationTimeoutMs;
    int64_t postWriteWait = args.postWriteWaitMs;
    int64_t startEpoch = args.startEpochMs;
    int64_t interval = args.intervalMs;
    if (!readInteger("payload-bytes", 1, static_cast<int64_t>(std::numeric_limits<int32_t>::max()), payload, true)
        || !readInteger("repeat", 0, 1000000, repeat, true)
        || !readInteger("warmup", 0, 1000000, warmup, true)
        || !readInteger("iterations", 1, 1000000, iterations, true)
        || !readInteger("request-timeout-ms", 1, std::numeric_limits<int32_t>::max(), requestTimeout, false)
        || !readInteger("location-timeout-ms", 1, std::numeric_limits<int32_t>::max(), locationTimeout, false)
        || !readInteger("post-write-wait-ms", 0, std::numeric_limits<int32_t>::max(), postWriteWait, false)
        || !readInteger("start-epoch-ms", 0, std::numeric_limits<int64_t>::max(), startEpoch, false)
        || !readInteger("interval-ms", 0, std::numeric_limits<int32_t>::max(), interval, false)) {
        return false;
    }
    args.payloadBytes = static_cast<uint64_t>(payload);
    args.repeat = static_cast<int>(repeat);
    args.warmup = static_cast<int>(warmup);
    args.iterations = static_cast<int>(iterations);
    args.requestTimeoutMs = static_cast<int>(requestTimeout);
    args.locationTimeoutMs = static_cast<int>(locationTimeout);
    args.postWriteWaitMs = static_cast<int>(postWriteWait);
    args.startEpochMs = startEpoch;
    args.intervalMs = static_cast<int>(interval);

    static const std::unordered_set<std::string> tasks{ "writer", "reader", "inspect", "cleanup" };
    if (tasks.count(args.task) == 0) {
        std::cerr << "Unknown --task: " << args.task << std::endl;
        return false;
    }
    if (!IsSafeIdentifier(args.runId) || !IsSafeIdentifier(args.scenario) || !IsSafeIdentifier(args.buildLabel)
        || !IsSafeIdentifier(args.clientId)
        || (!args.comparisonId.empty() && !IsSafeIdentifier(args.comparisonId))
        || (!args.topologyId.empty() && !IsSafeIdentifier(args.topologyId))) {
        std::cerr << "run-id, scenario, build-label, client-id, comparison-id, and topology-id must use "
                     "[A-Za-z0-9_.-] only"
                  << std::endl;
        return false;
    }
    return true;
}

bool ParseWorker(const std::string &worker, std::string &host, int32_t &port)
{
    const auto separator = worker.rfind(':');
    if (separator == std::string::npos || separator == 0 || separator + 1 >= worker.size()) {
        std::cerr << "Invalid --worker, expected HOST:PORT: " << worker << std::endl;
        return false;
    }
    host = worker.substr(0, separator);
    int64_t parsedPort = 0;
    if (!ParseInteger("worker", worker.substr(separator + 1), 1, 65535, parsedPort)) {
        return false;
    }
    port = static_cast<int32_t>(parsedPort);
    return true;
}

std::string RemoteClientId(const Arguments &args)
{
    std::string id =
        "ma_" + args.runId + "_" + args.scenario + "_" + std::to_string(args.payloadBytes) + "_"
        + std::to_string(args.repeat);
    std::replace_if(id.begin(), id.end(), [](unsigned char ch) { return !std::isalnum(ch) && ch != '_'; }, '_');
    return id;
}

std::vector<std::string> LoadKeys(const Arguments &args)
{
    std::vector<std::string> keys;
    if (!args.keysFile.empty()) {
        std::ifstream input(args.keysFile);
        if (!input.is_open()) {
            throw std::runtime_error("Cannot open keys file: " + args.keysFile);
        }
        std::string line;
        while (std::getline(input, line)) {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (!line.empty()) {
                keys.emplace_back(std::move(line));
            }
        }
    } else {
        const int total = args.warmup + args.iterations;
        keys.reserve(static_cast<size_t>(total));
        const std::string prefix = "ma_" + args.runId + "_" + args.scenario + "_"
                                   + std::to_string(args.payloadBytes) + "_r" + std::to_string(args.repeat) + "_";
        for (int i = 0; i < total; ++i) {
            keys.emplace_back(prefix + (i < args.warmup ? "w" : "m") + std::to_string(i));
        }
    }
    const size_t expected = static_cast<size_t>(args.warmup + args.iterations);
    if (keys.size() != expected) {
        throw std::runtime_error("Expected " + std::to_string(expected) + " keys, got " + std::to_string(keys.size()));
    }
    std::unordered_set<std::string> unique(keys.begin(), keys.end());
    if (unique.size() != keys.size()) {
        throw std::runtime_error("Key manifest contains duplicate keys");
    }
    return keys;
}

std::string KeySetDigest(const std::vector<std::string> &keys)
{
    // FNV-1a is used as a stable workload identity, not as a security primitive.
    uint64_t digest = 14695981039346656037ULL;
    for (const auto &key : keys) {
        for (unsigned char ch : key) {
            digest ^= ch;
            digest *= 1099511628211ULL;
        }
        digest ^= 0xff;
        digest *= 1099511628211ULL;
    }
    std::ostringstream output;
    output << std::hex << std::setfill('0') << std::setw(16) << digest;
    return output.str();
}

std::vector<uint8_t> BuildPayload(uint64_t size)
{
    std::vector<uint8_t> payload(static_cast<size_t>(size));
    for (uint64_t i = 0; i < size; ++i) {
        payload[static_cast<size_t>(i)] = static_cast<uint8_t>((i * 131 + size) % 251);
    }
    return payload;
}

int64_t SystemEpochMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void AppendResult(const Arguments &args, const std::string &kind, int iteration, bool warmup, int64_t latencyNs,
                  const Status &status, const std::string &extra = "", int64_t completedEpochMs = 0)
{
    std::ofstream output(args.output, std::ios::app);
    if (!output.is_open()) {
        throw std::runtime_error("Cannot open result file: " + args.output);
    }
    if (completedEpochMs == 0) {
        completedEpochMs = SystemEpochMs();
    }
    output << "{\"kind\":\"" << JsonEscape(kind) << "\",\"build\":\"" << JsonEscape(args.buildLabel)
           << "\",\"run_id\":\"" << JsonEscape(args.runId) << "\",\"scenario\":\""
           << JsonEscape(args.scenario) << "\",\"client_id\":\"" << JsonEscape(args.clientId)
           << "\",\"comparison_id\":\"" << JsonEscape(args.comparisonId) << "\",\"topology_id\":\""
           << JsonEscape(args.topologyId) << "\",\"key_set_digest\":\"" << JsonEscape(args.keySetDigest)
           << "\",\"enable_local_cache\":true,\"payload_bytes\":" << args.payloadBytes
           << ",\"repeat\":" << args.repeat << ",\"iteration\":" << iteration
           << ",\"warmup\":" << (warmup ? "true" : "false") << ",\"latency_ns\":" << latencyNs
           << ",\"status_code\":" << static_cast<uint32_t>(status.GetCode()) << ",\"status\":\""
           << JsonEscape(status.ToString()) << "\",\"completed_epoch_ms\":" << completedEpochMs;
    if (!extra.empty()) {
        output << ",\"extra\":\"" << JsonEscape(extra) << "\"";
    }
    output << "}\n";
}

Status ValidateBuffer(Optional<Buffer> &optionalBuffer, const std::vector<uint8_t> &payload)
{
    if (!optionalBuffer) {
        return Status(datasystem::K_NOT_FOUND, "Get returned an empty buffer");
    }
    Buffer &buffer = optionalBuffer.value();
    if (buffer.GetSize() != static_cast<int64_t>(payload.size())) {
        return Status(datasystem::K_DATA_INCONSISTENCY, "Get returned an unexpected payload size");
    }
    if (payload.empty()) {
        return Status::OK();
    }
    return buffer.CopyDataWithRLatch([&]() {
        const auto *data = static_cast<const uint8_t *>(buffer.ImmutableData());
        if (data == nullptr || data[0] != payload.front() || data[payload.size() - 1] != payload.back()) {
            return Status(datasystem::K_DATA_INCONSISTENCY, "Get payload boundary bytes do not match");
        }
        return Status::OK();
    });
}

bool ContainsLocation(const ObjMetaInfo &meta, const std::string &location)
{
    return std::find(meta.locations.begin(), meta.locations.end(), location) != meta.locations.end();
}

Status GetObjMetaInfoBatched(ObjectClient &client, const std::vector<std::string> &keys,
                             std::vector<ObjMetaInfo> &metas)
{
    metas.clear();
    metas.reserve(keys.size());
    for (size_t begin = 0; begin < keys.size(); begin += MAX_GET_OBJ_META_INFO_KEYS) {
        const size_t end = std::min(keys.size(), begin + MAX_GET_OBJ_META_INFO_KEYS);
        std::vector<std::string> batch(keys.begin() + static_cast<std::ptrdiff_t>(begin),
                                       keys.begin() + static_cast<std::ptrdiff_t>(end));
        std::vector<ObjMetaInfo> batchMetas;
        const Status status = client.GetObjMetaInfo("", batch, batchMetas);
        if (status.IsError()) {
            return status;
        }
        if (batchMetas.size() != batch.size()) {
            return Status(datasystem::K_DATA_INCONSISTENCY,
                          "GetObjMetaInfo returned " + std::to_string(batchMetas.size()) + " entries for "
                              + std::to_string(batch.size()) + " keys");
        }
        metas.insert(metas.end(), std::make_move_iterator(batchMetas.begin()),
                     std::make_move_iterator(batchMetas.end()));
    }
    return Status::OK();
}

Status WaitForExpectedLocation(ObjectClient &client, const std::vector<std::string> &keys,
                               const std::string &expectedLocation, int timeoutMs)
{
    if (expectedLocation.empty()) {
        return Status::OK();
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeoutMs);
    std::unordered_set<std::string> pending(keys.begin(), keys.end());
    while (!pending.empty() && Clock::now() < deadline) {
        std::vector<std::string> batch(pending.begin(), pending.end());
        std::vector<ObjMetaInfo> metas;
        const Status status = GetObjMetaInfoBatched(client, batch, metas);
        if (status.IsOk() && metas.size() == batch.size()) {
            for (size_t i = 0; i < batch.size(); ++i) {
                if (ContainsLocation(metas[i], expectedLocation)) {
                    pending.erase(batch[i]);
                }
            }
        }
        if (!pending.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
    if (!pending.empty()) {
        return Status(datasystem::K_WORKER_TIMEOUT,
                      std::to_string(pending.size()) + " keys did not reach expected location " + expectedLocation);
    }
    return Status::OK();
}

int RunWriter(ObjectClient &client, const Arguments &args, const std::vector<std::string> &keys)
{
    const std::vector<uint8_t> payload = BuildPayload(args.payloadBytes);
    const CreateParam createParam;
    std::vector<OperationSample> samples;
    samples.reserve(keys.size());
    int errors = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto start = Clock::now();
        const Status status = client.Put(keys[i], payload.data(), payload.size(), createParam);
        const int64_t latencyNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
        const bool isWarmup = i < static_cast<size_t>(args.warmup);
        samples.push_back(
            OperationSample{ static_cast<int>(i), isWarmup, latencyNs, status, SystemEpochMs() });
        if (status.IsError()) {
            ++errors;
        }
    }
    for (const auto &sample : samples) {
        AppendResult(args, "put", sample.iteration, sample.warmup, sample.latencyNs, sample.status, "",
                     sample.completedEpochMs);
    }
    if (errors != 0) {
        return 2;
    }

    std::vector<std::string> failedKeys;
    const Status refStatus = client.GIncreaseRef(keys, failedKeys, RemoteClientId(args));
    if (refStatus.IsError() || !failedKeys.empty()) {
        std::cerr << "GIncreaseRef failed: " << refStatus.ToString() << ", failed keys: " << failedKeys.size()
                  << std::endl;
        return 3;
    }
    const Status locationStatus =
        WaitForExpectedLocation(client, keys, args.expectedLocation, args.locationTimeoutMs);
    if (locationStatus.IsError()) {
        std::cerr << "Location verification failed: " << locationStatus.ToString() << std::endl;
        return 4;
    }
    if (args.postWriteWaitMs > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(args.postWriteWaitMs));
    }
    return 0;
}

void WaitUntilEpochMs(int64_t epochMs)
{
    if (epochMs <= 0) {
        return;
    }
    const auto target = std::chrono::system_clock::time_point(std::chrono::milliseconds(epochMs));
    const auto now = std::chrono::system_clock::now();
    if (target > now) {
        std::this_thread::sleep_until(target);
    }
}

int RunReader(ObjectClient &client, const Arguments &args, const std::vector<std::string> &keys)
{
    const std::vector<uint8_t> expectedPayload = BuildPayload(args.payloadBytes);
    std::vector<OperationSample> samples;
    samples.reserve(static_cast<size_t>(args.iterations));
    int errors = 0;
    for (int i = 0; i < args.warmup; ++i) {
        std::vector<Optional<Buffer>> buffers;
        const Status getStatus = client.Get({ keys[static_cast<size_t>(i)] }, 0, buffers);
        Status status = getStatus;
        if (status.IsOk()) {
            if (buffers.size() != 1) {
                status = Status(datasystem::K_DATA_INCONSISTENCY, "Get returned an unexpected buffer count");
            } else {
                status = ValidateBuffer(buffers[0], expectedPayload);
            }
        }
        if (status.IsError()) {
            std::cerr << "Warmup Get failed for " << keys[static_cast<size_t>(i)] << ": " << status.ToString()
                      << std::endl;
            return 2;
        }
    }

    WaitUntilEpochMs(args.startEpochMs);
    const auto scheduleStart = Clock::now();
    for (int i = 0; i < args.iterations; ++i) {
        if (args.intervalMs > 0) {
            std::this_thread::sleep_until(scheduleStart + std::chrono::milliseconds(
                                                             static_cast<int64_t>(i) * args.intervalMs));
        }
        const size_t keyIndex = static_cast<size_t>(args.warmup + i);
        std::vector<Optional<Buffer>> buffers;
        const auto start = Clock::now();
        const Status getStatus = client.Get({ keys[keyIndex] }, 0, buffers);
        const int64_t latencyNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
        Status status = getStatus;
        if (status.IsOk()) {
            if (buffers.size() != 1) {
                status = Status(datasystem::K_DATA_INCONSISTENCY, "Get returned an unexpected buffer count");
            } else {
                status = ValidateBuffer(buffers[0], expectedPayload);
            }
        }
        samples.push_back(OperationSample{ i, false, latencyNs, status, SystemEpochMs() });
        if (status.IsError()) {
            ++errors;
        }
    }
    for (const auto &sample : samples) {
        AppendResult(args, "cold_get", sample.iteration, sample.warmup, sample.latencyNs, sample.status, "",
                     sample.completedEpochMs);
    }
    return errors == 0 ? 0 : 3;
}

int RunInspect(ObjectClient &client, const Arguments &args, const std::vector<std::string> &keys)
{
    std::vector<ObjMetaInfo> metas;
    const Status status = GetObjMetaInfoBatched(client, keys, metas);
    if (status.IsError()) {
        std::cerr << status.ToString() << std::endl;
        return 2;
    }
    if (metas.size() != keys.size()) {
        std::cerr << "GetObjMetaInfo returned " << metas.size() << " entries for " << keys.size() << " keys"
                  << std::endl;
        return 3;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        std::ostringstream locations;
        for (size_t j = 0; j < metas[i].locations.size(); ++j) {
            if (j != 0) {
                locations << ',';
            }
            locations << metas[i].locations[j];
        }
        AppendResult(args, "location", static_cast<int>(i), i < static_cast<size_t>(args.warmup), 0, Status::OK(),
                     locations.str());
    }
    return 0;
}

int RunCleanup(ObjectClient &client, const Arguments &args)
{
    const Status status = client.ReleaseGRefs(RemoteClientId(args));
    if (status.IsError()) {
        std::cerr << "ReleaseGRefs failed: " << status.ToString() << std::endl;
        return 2;
    }
    return 0;
}

}  // namespace

int main(int argc, char **argv)
{
    Arguments args;
    if (!ParseArguments(argc, argv, args)) {
        PrintUsage(argv[0]);
        return 1;
    }

    try {
        std::string host;
        int32_t port = 0;
        if (!ParseWorker(args.worker, host, port)) {
            return 1;
        }
        ConnectOptions options;
        options.host = host;
        options.port = port;
        options.connectTimeoutMs = args.requestTimeoutMs;
        options.requestTimeoutMs = args.requestTimeoutMs;
        options.enableLocalCache = true;

        ObjectClient client(options);
        const Status initStatus = client.Init();
        if (initStatus.IsError()) {
            std::cerr << "ObjectClient::Init failed: " << initStatus.ToString() << std::endl;
            return 2;
        }
        const std::vector<std::string> keys = LoadKeys(args);
        args.keySetDigest = KeySetDigest(keys);

        int result = 0;
        if (args.task == "writer") {
            result = RunWriter(client, args, keys);
        } else if (args.task == "reader") {
            result = RunReader(client, args, keys);
        } else if (args.task == "inspect") {
            result = RunInspect(client, args, keys);
        } else {
            result = RunCleanup(client, args);
        }
        const Status shutdownStatus = client.ShutDown();
        if (shutdownStatus.IsError()) {
            std::cerr << "ObjectClient::ShutDown failed: " << shutdownStatus.ToString() << std::endl;
            return result == 0 ? 3 : result;
        }
        return result;
    } catch (const std::exception &error) {
        std::cerr << error.what() << std::endl;
        return 10;
    }
}
