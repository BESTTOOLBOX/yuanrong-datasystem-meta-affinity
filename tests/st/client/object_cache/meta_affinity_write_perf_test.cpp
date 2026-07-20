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

/**
 * Description: Optional Put/Get latency benchmarks for meta-affinity write (DS_META_AFFINITY_WRITE_PERF=1).
 *
 * Put: compares colocated vs cross-worker / remote-only write paths.
 * Get: after direct meta-owner write, primary is colocated with meta from the first read.
 * RPC mode (DS_META_AFFINITY_WRITE_PERF_RPC=1): small payload (4KB default), focused Get hop reduction checks.
 */
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "meta_affinity_st_fixtures.h"

namespace datasystem {
namespace st {
namespace {

struct LatencyReport {
    size_t sampleCount = 0;
    double avgUs = 0;
    double p99Us = 0;
    double p9999Us = 0;
};

struct PutWritePerfSample {
    double putUs = 0;
    double getUs = 0;
    double primaryReadyUs = 0;
    bool primaryOnMetaOwnerAtPutReturn = false;
};

int EnvIntOrDefault(const char *name, int defaultValue)
{
    const char *value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return defaultValue;
    }
    return std::max(0, std::atoi(value));
}

bool MetaAffinityWritePerfEnabled()
{
    const char *value = std::getenv("DS_META_AFFINITY_WRITE_PERF");
    return value != nullptr && *value != '\0' && std::string(value) != "0";
}

bool MetaAffinityWritePerfAssertEnabled()
{
    const char *value = std::getenv("DS_META_AFFINITY_WRITE_PERF_ASSERT");
    return value != nullptr && *value != '\0' && std::string(value) != "0";
}

bool ShouldRunMetaAffinityWritePerfLocalScenario()
{
    const char *mode = std::getenv("DS_META_AFFINITY_WRITE_PERF_MODE");
    return mode == nullptr || std::string(mode) == "all" || std::string(mode) == "local";
}

bool ShouldRunMetaAffinityWritePerfRemoteScenario()
{
    const char *mode = std::getenv("DS_META_AFFINITY_WRITE_PERF_MODE");
    return mode == nullptr || std::string(mode) == "all" || std::string(mode) == "remote";
}

bool ShouldRunMetaAffinityWritePutPerf()
{
    const char *op = std::getenv("DS_META_AFFINITY_WRITE_PERF_OP");
    return op == nullptr || std::string(op) == "all" || std::string(op) == "put";
}

bool ShouldRunMetaAffinityWriteGetPerf()
{
    const char *op = std::getenv("DS_META_AFFINITY_WRITE_PERF_OP");
    return op == nullptr || std::string(op) == "all" || std::string(op) == "get";
}

bool ShouldRunGetRpcReductionPerf()
{
    const char *value = std::getenv("DS_META_AFFINITY_WRITE_PERF_RPC");
    return value != nullptr && *value != '\0' && std::string(value) != "0";
}

double PercentileUs(const std::vector<double> &sortedUs, double percentile)
{
    if (sortedUs.empty()) {
        return 0;
    }
    const double rank = (percentile / 100.0) * static_cast<double>(sortedUs.size() - 1);
    const size_t lower = static_cast<size_t>(std::floor(rank));
    const size_t upper = static_cast<size_t>(std::ceil(rank));
    if (lower >= sortedUs.size()) {
        return sortedUs.back();
    }
    if (lower == upper) {
        return sortedUs[lower];
    }
    const double weight = rank - static_cast<double>(lower);
    return sortedUs[lower] * (1.0 - weight) + sortedUs[upper] * weight;
}

LatencyReport SummarizeLatency(const std::vector<double> &samplesUs)
{
    LatencyReport report;
    report.sampleCount = samplesUs.size();
    if (samplesUs.empty()) {
        return report;
    }
    std::vector<double> sorted = samplesUs;
    std::sort(sorted.begin(), sorted.end());
    double sum = 0;
    for (double sample : sorted) {
        sum += sample;
    }
    report.avgUs = sum / static_cast<double>(sorted.size());
    report.p99Us = PercentileUs(sorted, 99.0);
    report.p9999Us = PercentileUs(sorted, 99.99);
    return report;
}

bool IsPrimaryOnMetaOwner(MetaAffinityStFixture *fixture, const std::string &objectKey, uint32_t metaOwnerWorkerIndex)
{
    return fixture->IsPrimaryOnWorker(objectKey, metaOwnerWorkerIndex);
}

double MeasureOneGet(const std::shared_ptr<ObjectClient> &client, const std::string &objectKey)
{
    const auto getStart = std::chrono::steady_clock::now();
    std::vector<Optional<Buffer>> buffers;
    const Status getStatus = client->Get({ objectKey }, 0, buffers);
    if (getStatus.IsError()) {
        ADD_FAILURE() << "Get failed for " << objectKey << ": " << getStatus.ToString();
        return 0;
    }
    return std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - getStart).count();
}

void PutObject(MetaAffinityStFixture *fixture, const std::shared_ptr<ObjectClient> &client,
               const std::string &objectKey, const std::string &payload, uint32_t metaOwnerWorkerIndex,
               bool waitForPrimaryColocation, PutWritePerfSample *sampleOut)
{
    CreateParam param;
    const auto putStart = std::chrono::steady_clock::now();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<const uint8_t *>(payload.data()), payload.size(), param));
    const auto putEnd = std::chrono::steady_clock::now();

    const bool primaryAtPutReturn = IsPrimaryOnMetaOwner(fixture, objectKey, metaOwnerWorkerIndex);
    double primaryReadyUs = 0;
    if (!primaryAtPutReturn && waitForPrimaryColocation) {
        const auto readyStart = std::chrono::steady_clock::now();
        fixture->WaitUntilPrimaryOnWorker(objectKey, metaOwnerWorkerIndex);
        primaryReadyUs =
            std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - readyStart).count();
    }

    if (sampleOut != nullptr) {
        sampleOut->putUs = std::chrono::duration<double, std::micro>(putEnd - putStart).count();
        sampleOut->primaryReadyUs = primaryReadyUs;
        sampleOut->primaryOnMetaOwnerAtPutReturn = primaryAtPutReturn;
    }
}

void MeasureColdPuts(MetaAffinityStFixture *fixture, const std::shared_ptr<ObjectClient> &client,
                     const std::vector<std::string> &objectKeys, const std::string &payload,
                     uint32_t metaOwnerWorkerIndex, size_t warmupIters, size_t measureIters,
                     bool waitForPrimaryColocation, std::vector<PutWritePerfSample> &samples)
{
    ASSERT_GE(objectKeys.size(), warmupIters + measureIters);
    samples.clear();
    samples.reserve(measureIters);
    for (size_t i = 0; i < warmupIters + measureIters; ++i) {
        PutWritePerfSample sample;
        PutObject(fixture, client, objectKeys[i], payload, metaOwnerWorkerIndex, waitForPrimaryColocation, &sample);
        if (i >= warmupIters) {
            samples.push_back(sample);
        }
    }
}

void PopulateObjects(MetaAffinityStFixture *fixture, const std::shared_ptr<ObjectClient> &writeClient,
                     const std::vector<std::string> &objectKeys, const std::string &payload,
                     uint32_t metaOwnerWorkerIndex, bool waitForPrimaryColocation)
{
    for (const auto &objectKey : objectKeys) {
        PutObject(fixture, writeClient, objectKey, payload, metaOwnerWorkerIndex, waitForPrimaryColocation, nullptr);
    }
}

void MeasureColdGets(const std::shared_ptr<ObjectClient> &readClient, const std::vector<std::string> &objectKeys,
                     size_t warmupIters, size_t measureIters, std::vector<double> &getSamplesUs)
{
    ASSERT_GE(objectKeys.size(), warmupIters + measureIters);
    getSamplesUs.clear();
    getSamplesUs.reserve(measureIters);
    for (size_t i = 0; i < warmupIters + measureIters; ++i) {
        const double getUs = MeasureOneGet(readClient, objectKeys[i]);
        if (i >= warmupIters) {
            getSamplesUs.push_back(getUs);
        }
    }
}

void MeasurePutThenImmediateGets(MetaAffinityStFixture *fixture, const std::shared_ptr<ObjectClient> &client,
                                 const std::vector<std::string> &objectKeys, const std::string &payload,
                                 uint32_t metaOwnerWorkerIndex, size_t warmupIters, size_t measureIters,
                                 bool waitForPrimaryBeforeGet, std::vector<PutWritePerfSample> &samples)
{
    ASSERT_GE(objectKeys.size(), warmupIters + measureIters);
    samples.clear();
    samples.reserve(measureIters);
    for (size_t i = 0; i < warmupIters + measureIters; ++i) {
        const auto &objectKey = objectKeys[i];
        PutWritePerfSample sample;
        PutObject(fixture, client, objectKey, payload, metaOwnerWorkerIndex, false, &sample);
        if (waitForPrimaryBeforeGet && !sample.primaryOnMetaOwnerAtPutReturn) {
            const auto readyStart = std::chrono::steady_clock::now();
            fixture->WaitUntilPrimaryOnWorker(objectKey, metaOwnerWorkerIndex);
            sample.primaryReadyUs =
                std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - readyStart).count();
        }
        sample.getUs = MeasureOneGet(client, objectKey);
        if (i >= warmupIters) {
            samples.push_back(sample);
        }
    }
}

void PrintMetaAffinityWritePerfJson(const std::string &scenario, size_t payloadBytes, size_t warmup, size_t iters,
                                    const LatencyReport &putReport, const LatencyReport &getReport,
                                    const LatencyReport &primaryReadyReport, size_t immediateAtPutReturnCount)
{
    std::ostringstream os;
    os << std::fixed << std::setprecision(3);
    os << "{\"scenario\":\"" << scenario << "\",\"payload_bytes\":" << payloadBytes << ",\"warmup\":" << warmup
       << ",\"iters\":" << iters << ",\"put_count\":" << putReport.sampleCount << ",\"put_avg_us\":" << putReport.avgUs
       << ",\"put_p99_us\":" << putReport.p99Us << ",\"put_p9999_us\":" << putReport.p9999Us
       << ",\"get_count\":" << getReport.sampleCount << ",\"get_avg_us\":" << getReport.avgUs
       << ",\"get_p99_us\":" << getReport.p99Us << ",\"get_p9999_us\":" << getReport.p9999Us
       << ",\"primary_ready_avg_us\":" << primaryReadyReport.avgUs
       << ",\"primary_ready_p99_us\":" << primaryReadyReport.p99Us
       << ",\"primary_immediate_at_put_return_count\":" << immediateAtPutReturnCount << "}";
    std::cout << "META_AFFINITY_WRITE_PERF_JSON=" << os.str() << std::endl;
}

void RunPutPerfScenario(MetaAffinityStFixture *fixture, const std::shared_ptr<ObjectClient> &client,
                        uint32_t metaOwnerWorkerIndex, const std::string &payload, size_t payloadBytes, size_t warmup,
                        size_t iters, const std::string &scenario, bool waitForPrimaryColocation,
                        bool useClientRouteKeys)
{
    std::vector<std::string> objectKeys;
    if (useClientRouteKeys) {
        fixture->GetClientRouteKeysForMetaOwner(metaOwnerWorkerIndex, warmup + iters, objectKeys);
    } else {
        fixture->GetHashKeysForMetaOwner(metaOwnerWorkerIndex, warmup + iters, objectKeys);
    }

    std::vector<PutWritePerfSample> samples;
    MeasureColdPuts(fixture, client, objectKeys, payload, metaOwnerWorkerIndex, warmup, iters, waitForPrimaryColocation,
                    samples);

    std::vector<double> putSamples;
    std::vector<double> primaryReadySamples;
    putSamples.reserve(samples.size());
    primaryReadySamples.reserve(samples.size());
    size_t immediateAtPutReturnCount = 0;
    for (const auto &sample : samples) {
        putSamples.push_back(sample.putUs);
        primaryReadySamples.push_back(sample.primaryReadyUs);
        if (sample.primaryOnMetaOwnerAtPutReturn) {
            ++immediateAtPutReturnCount;
        }
    }

    PrintMetaAffinityWritePerfJson(scenario, payloadBytes, warmup, iters, SummarizeLatency(putSamples), LatencyReport{},
                                   SummarizeLatency(primaryReadySamples), immediateAtPutReturnCount);

    if (MetaAffinityWritePerfAssertEnabled()) {
        if (scenario.find("direct_meta_owner") != std::string::npos) {
            EXPECT_EQ(immediateAtPutReturnCount, samples.size()) << scenario;
        }
        if (scenario.find("gateway_replicate") != std::string::npos
            || scenario.find("cross_worker_replicate") != std::string::npos) {
            EXPECT_EQ(immediateAtPutReturnCount, 0u) << scenario;
        }
    }
}

void RunSteadyGetPerfScenario(MetaAffinityStFixture *fixture, const std::shared_ptr<ObjectClient> &writeClient,
                              const std::shared_ptr<ObjectClient> &readClient, uint32_t metaOwnerWorkerIndex,
                              const std::string &payload, size_t payloadBytes, size_t warmup, size_t iters,
                              const std::string &scenario, bool waitForPrimaryAfterWrite, bool useClientRouteKeys)
{
    std::vector<std::string> objectKeys;
    if (useClientRouteKeys) {
        fixture->GetClientRouteKeysForMetaOwner(metaOwnerWorkerIndex, warmup + iters, objectKeys);
    } else {
        fixture->GetHashKeysForMetaOwner(metaOwnerWorkerIndex, warmup + iters, objectKeys);
    }

    PopulateObjects(fixture, writeClient, objectKeys, payload, metaOwnerWorkerIndex, waitForPrimaryAfterWrite);

    std::vector<double> getSamplesUs;
    MeasureColdGets(readClient, objectKeys, warmup, iters, getSamplesUs);

    PrintMetaAffinityWritePerfJson(scenario, payloadBytes, warmup, iters, LatencyReport{},
                                   SummarizeLatency(getSamplesUs), LatencyReport{}, 0);
}

void RunImmediatePutGetPerfScenario(MetaAffinityStFixture *fixture, const std::shared_ptr<ObjectClient> &client,
                                    uint32_t metaOwnerWorkerIndex, const std::string &payload, size_t payloadBytes,
                                    size_t warmup, size_t iters, const std::string &scenario,
                                    bool waitForPrimaryBeforeGet, bool useClientRouteKeys)
{
    std::vector<std::string> objectKeys;
    if (useClientRouteKeys) {
        fixture->GetClientRouteKeysForMetaOwner(metaOwnerWorkerIndex, warmup + iters, objectKeys);
    } else {
        fixture->GetHashKeysForMetaOwner(metaOwnerWorkerIndex, warmup + iters, objectKeys);
    }

    std::vector<PutWritePerfSample> samples;
    MeasurePutThenImmediateGets(fixture, client, objectKeys, payload, metaOwnerWorkerIndex, warmup, iters,
                                waitForPrimaryBeforeGet, samples);

    std::vector<double> putSamples;
    std::vector<double> getSamples;
    std::vector<double> primaryReadySamples;
    size_t immediateAtPutReturnCount = 0;
    for (const auto &sample : samples) {
        putSamples.push_back(sample.putUs);
        getSamples.push_back(sample.getUs);
        primaryReadySamples.push_back(sample.primaryReadyUs);
        if (sample.primaryOnMetaOwnerAtPutReturn) {
            ++immediateAtPutReturnCount;
        }
    }

    PrintMetaAffinityWritePerfJson(scenario, payloadBytes, warmup, iters, SummarizeLatency(putSamples),
                                   SummarizeLatency(getSamples), SummarizeLatency(primaryReadySamples),
                                   immediateAtPutReturnCount);

    if (MetaAffinityWritePerfAssertEnabled()) {
        if (scenario.find("direct_meta_owner") != std::string::npos && scenario.find("gateway") == std::string::npos) {
            EXPECT_EQ(immediateAtPutReturnCount, samples.size()) << scenario;
        }
    }
}

void RunGatewayPutRemoteImmediateGetScenario(MetaAffinityStFixture *fixture,
                                             const std::shared_ptr<ObjectClient> &gatewayWriter,
                                             const std::shared_ptr<ObjectClient> &remoteReader,
                                             uint32_t metaOwnerWorkerIndex, const std::string &payload,
                                             size_t payloadBytes, size_t warmup, size_t iters,
                                             const std::string &scenario)
{
    std::vector<std::string> objectKeys;
    fixture->GetHashKeysForMetaOwner(metaOwnerWorkerIndex, warmup + iters, objectKeys);

    std::vector<PutWritePerfSample> samples;
    samples.reserve(iters);
    for (size_t i = 0; i < warmup + iters; ++i) {
        PutWritePerfSample sample;
        PutObject(fixture, gatewayWriter, objectKeys[i], payload, metaOwnerWorkerIndex, false, &sample);
        sample.getUs = MeasureOneGet(remoteReader, objectKeys[i]);
        if (i >= warmup) {
            samples.push_back(sample);
        }
    }

    std::vector<double> putSamples;
    std::vector<double> getSamples;
    for (const auto &sample : samples) {
        putSamples.push_back(sample.putUs);
        getSamples.push_back(sample.getUs);
    }

    PrintMetaAffinityWritePerfJson(scenario, payloadBytes, warmup, iters, SummarizeLatency(putSamples),
                                   SummarizeLatency(getSamples), LatencyReport{}, 0);
}

void RunSameNodeColdGetFromPrimary(MetaAffinityStFixture *fixture, size_t payloadBytes, size_t warmup, size_t iters)
{
    const std::string payload = RandomData().GetRandomString(static_cast<int64_t>(payloadBytes));
    std::vector<std::string> objectKeys;
    fixture->GetHashKeysForMetaOwner(1, warmup + iters, objectKeys);

    std::shared_ptr<ObjectClient> remoteClient;
    fixture->InitRemoteOnlyAffinityClient(remoteClient);
    FLAGS_enable_meta_affinity_replicate = true;
    PopulateObjects(fixture, remoteClient, objectKeys, payload, 1, false);

    std::shared_ptr<ObjectClient> client0;
    fixture->InitTestClient(0, client0);
    std::vector<double> directWriteGetSamples;
    MeasureColdGets(client0, objectKeys, warmup, iters, directWriteGetSamples);
    PrintMetaAffinityWritePerfJson("same_node_cold_get_after_direct_remote_write", payloadBytes, warmup, iters,
                                   LatencyReport{}, SummarizeLatency(directWriteGetSamples), LatencyReport{}, 0);

    std::vector<std::string> replicateKeys;
    fixture->GetHashKeysForMetaOwner(1, warmup + iters, replicateKeys);
    std::shared_ptr<ObjectClient> writer0;
    fixture->InitTestClient(0, writer0);
    CreateParam param;
    for (const auto &objectKey : replicateKeys) {
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(writer0->Create(objectKey, payload.size(), param, buffer));
        DS_ASSERT_OK(buffer->WLatch());
        DS_ASSERT_OK(buffer->MemoryCopy(payload.data(), payload.size()));
        DS_ASSERT_OK(buffer->Seal({}));
        DS_ASSERT_OK(buffer->UnWLatch());
        fixture->WaitUntilPrimaryOnWorker(objectKey, 1);
        std::vector<Optional<Buffer>> warmBuffers;
        DS_ASSERT_OK(writer0->Get({ objectKey }, 0, warmBuffers));
        ASSERT_EQ(warmBuffers.size(), 1u);
        ASSERT_TRUE(warmBuffers[0]);
        DS_ASSERT_OK(warmBuffers[0]->InvalidateBuffer());
    }

    std::vector<double> replicateWriteGetSamples;
    MeasureColdGets(client0, replicateKeys, warmup, iters, replicateWriteGetSamples);
    PrintMetaAffinityWritePerfJson("same_node_cold_get_after_local_replicate", payloadBytes, warmup, iters,
                                   LatencyReport{}, SummarizeLatency(replicateWriteGetSamples), LatencyReport{}, 0);
}

void RunSameNodePutBenchmarks(MetaAffinityStFixture *fixture, size_t payloadBytes, size_t warmup, size_t iters)
{
    std::shared_ptr<ObjectClient> client;
    fixture->InitTestClient(0, client);
    const std::string payload = RandomData().GetRandomString(static_cast<int64_t>(payloadBytes));

    RunPutPerfScenario(fixture, client, 0, payload, payloadBytes, warmup, iters, "same_node_local_meta_colocated",
                       false, false);
    RunPutPerfScenario(fixture, client, 1, payload, payloadBytes, warmup, iters, "same_node_cross_worker_replicate",
                       true, false);
}

void RunSameNodeGetBenchmarks(MetaAffinityStFixture *fixture, size_t payloadBytes, size_t warmup, size_t iters)
{
    RunSameNodeColdGetFromPrimary(fixture, payloadBytes, warmup, iters);
}

void RunCrossNodePutBenchmarks(MetaAffinityStFixture *fixture, size_t payloadBytes, size_t warmup, size_t iters)
{
    const std::string payload = RandomData().GetRandomString(static_cast<int64_t>(payloadBytes));

    std::shared_ptr<ObjectClient> directClient;
    fixture->InitRemoteOnlyAffinityClient(directClient);
    FLAGS_enable_meta_affinity_replicate = true;
    RunPutPerfScenario(fixture, directClient, 1, payload, payloadBytes, warmup, iters,
                       "cross_node_remote_direct_meta_owner", false, true);

    std::shared_ptr<ObjectClient> gatewayClient;
    fixture->InitTestClient(0, gatewayClient);
    FLAGS_enable_meta_affinity_replicate = true;
    fixture->DelayMetaAffinityReplicateOnWorker0(100);
    RunPutPerfScenario(fixture, gatewayClient, 1, payload, payloadBytes, warmup, iters, "cross_node_gateway_replicate",
                       false, false);
    fixture->ClearMetaAffinityReplicateDelayOnWorker0();
    FLAGS_enable_meta_affinity_replicate = true;
}

void PrintMetaAffinityGetRpcReductionSummary(
    size_t payloadBytes, size_t warmup, size_t iters, const LatencyReport &sameNodeColocatedImmediateGet,
    const LatencyReport &sameNodeCrossWorkerColdGet, const LatencyReport &sameNodeCrossWorkerImmediateGet,
    const LatencyReport &crossNodeGatewayColdGet, const LatencyReport &crossNodeDirectColdGet,
    const LatencyReport &crossNodeRemoteImmediateGet, const LatencyReport &crossNodeRemoteDirectColdGet,
    const LatencyReport &crossNodeRemoteGatewayColdGet)
{
    const auto pct = [](double baselineUs, double improvedUs) {
        return baselineUs > 0 ? (baselineUs - improvedUs) / baselineUs * 100.0 : 0;
    };
    std::ostringstream os;
    os << std::fixed << std::setprecision(3);
    os << "{\"payload_bytes\":" << payloadBytes << ",\"warmup\":" << warmup << ",\"iters\":" << iters
       << ",\"same_node_colocated_immediate_get_avg_us\":" << sameNodeColocatedImmediateGet.avgUs
       << ",\"same_node_cross_worker_cold_get_avg_us\":" << sameNodeCrossWorkerColdGet.avgUs
       << ",\"same_node_cross_worker_immediate_get_avg_us\":" << sameNodeCrossWorkerImmediateGet.avgUs
       << ",\"same_node_cold_get_improvement_pct\":"
       << pct(sameNodeCrossWorkerColdGet.avgUs, sameNodeColocatedImmediateGet.avgUs)
       << ",\"cross_node_gateway_cold_get_avg_us\":" << crossNodeGatewayColdGet.avgUs
       << ",\"cross_node_direct_cold_get_avg_us\":" << crossNodeDirectColdGet.avgUs
       << ",\"cross_node_remote_immediate_get_avg_us\":" << crossNodeRemoteImmediateGet.avgUs
       << ",\"cross_node_remote_direct_cold_get_avg_us\":" << crossNodeRemoteDirectColdGet.avgUs
       << ",\"cross_node_remote_gateway_cold_get_avg_us\":" << crossNodeRemoteGatewayColdGet.avgUs
       << ",\"cross_node_cold_get_improvement_pct\":"
       << pct(crossNodeGatewayColdGet.avgUs, crossNodeDirectColdGet.avgUs)
       << ",\"cross_node_remote_cold_get_improvement_pct\":"
       << pct(crossNodeRemoteGatewayColdGet.avgUs, crossNodeRemoteDirectColdGet.avgUs) << "}";
    std::cout << "META_AFFINITY_GET_RPC_REDUCTION_JSON=" << os.str() << std::endl;
}

std::vector<std::string> SliceObjectKeys(const std::vector<std::string> &allKeys, size_t slot, size_t slotSize)
{
    const size_t begin = slot * slotSize;
    EXPECT_GE(allKeys.size(), begin + slotSize) << "key pool too small for slot=" << slot;
    if (allKeys.size() < begin + slotSize) {
        return {};
    }
    return std::vector<std::string>(allKeys.begin() + static_cast<std::ptrdiff_t>(begin),
                                    allKeys.begin() + static_cast<std::ptrdiff_t>(begin + slotSize));
}

void MeasureClient0ColdGetAfterGatewayReplicateKeys(MetaAffinityStFixture *fixture, const std::string &payload,
                                                    const std::vector<std::string> &objectKeys, size_t warmup,
                                                    size_t iters, LatencyReport &report)
{
    std::shared_ptr<ObjectClient> writer0;
    fixture->InitTestClient(0, writer0);
    CreateParam param;
    for (const auto &objectKey : objectKeys) {
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(writer0->Create(objectKey, payload.size(), param, buffer));
        DS_ASSERT_OK(buffer->WLatch());
        DS_ASSERT_OK(buffer->MemoryCopy(payload.data(), payload.size()));
        DS_ASSERT_OK(buffer->Seal({}));
        DS_ASSERT_OK(buffer->UnWLatch());
        fixture->WaitUntilPrimaryOnWorker(objectKey, 1);
        std::vector<Optional<Buffer>> warmBuffers;
        DS_ASSERT_OK(writer0->Get({ objectKey }, 0, warmBuffers));
        ASSERT_EQ(warmBuffers.size(), 1u);
        ASSERT_TRUE(warmBuffers[0]);
        DS_ASSERT_OK(warmBuffers[0]->InvalidateBuffer());
    }

    std::vector<double> getSamplesUs;
    MeasureColdGets(writer0, objectKeys, warmup, iters, getSamplesUs);
    report = SummarizeLatency(getSamplesUs);
}

void MeasureClient0ColdGetAfterDirectRemoteWriteKeys(MetaAffinityStFixture *fixture, const std::string &payload,
                                                     const std::vector<std::string> &objectKeys, size_t warmup,
                                                     size_t iters, LatencyReport &report)
{
    std::shared_ptr<ObjectClient> remoteClient;
    fixture->InitRemoteOnlyAffinityClient(remoteClient);
    FLAGS_enable_meta_affinity_replicate = true;
    PopulateObjects(fixture, remoteClient, objectKeys, payload, 1, false);

    std::shared_ptr<ObjectClient> client0;
    fixture->InitTestClient(0, client0);
    std::vector<double> getSamplesUs;
    MeasureColdGets(client0, objectKeys, warmup, iters, getSamplesUs);
    report = SummarizeLatency(getSamplesUs);
}

void MeasureRemotePutGetImmediateDirectKeys(MetaAffinityStFixture *fixture, const std::string &payload,
                                            const std::vector<std::string> &objectKeys, size_t warmup, size_t iters,
                                            LatencyReport &report)
{
    std::shared_ptr<ObjectClient> remoteClient;
    fixture->InitRemoteOnlyAffinityClient(remoteClient);
    FLAGS_enable_meta_affinity_replicate = true;

    std::vector<PutWritePerfSample> samples;
    MeasurePutThenImmediateGets(fixture, remoteClient, objectKeys, payload, 1, warmup, iters, false, samples);

    std::vector<double> getSamplesUs;
    getSamplesUs.reserve(samples.size());
    for (const auto &sample : samples) {
        getSamplesUs.push_back(sample.getUs);
    }
    report = SummarizeLatency(getSamplesUs);
}

void MeasureRemoteColdGetAfterDirectWriteKeys(MetaAffinityStFixture *fixture, const std::string &payload,
                                              const std::vector<std::string> &objectKeys, size_t warmup, size_t iters,
                                              LatencyReport &report)
{
    std::shared_ptr<ObjectClient> remoteClient;
    fixture->InitRemoteOnlyAffinityClient(remoteClient);
    FLAGS_enable_meta_affinity_replicate = true;
    PopulateObjects(fixture, remoteClient, objectKeys, payload, 1, false);

    std::vector<double> getSamplesUs;
    MeasureColdGets(remoteClient, objectKeys, warmup, iters, getSamplesUs);
    report = SummarizeLatency(getSamplesUs);
}

void MeasureRemoteColdGetAfterGatewayReplicateKeys(MetaAffinityStFixture *fixture, const std::string &payload,
                                                   const std::vector<std::string> &objectKeys, size_t warmup,
                                                   size_t iters, LatencyReport &report)
{
    std::shared_ptr<ObjectClient> gatewayWriter;
    fixture->InitTestClient(0, gatewayWriter);
    CreateParam param;
    for (const auto &objectKey : objectKeys) {
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(gatewayWriter->Create(objectKey, payload.size(), param, buffer));
        DS_ASSERT_OK(buffer->WLatch());
        DS_ASSERT_OK(buffer->MemoryCopy(payload.data(), payload.size()));
        DS_ASSERT_OK(buffer->Seal({}));
        DS_ASSERT_OK(buffer->UnWLatch());
        fixture->WaitUntilPrimaryOnWorker(objectKey, 1);
    }

    std::shared_ptr<ObjectClient> remoteReader;
    fixture->InitRemoteOnlyAffinityClient(remoteReader);
    std::vector<double> getSamplesUs;
    MeasureColdGets(remoteReader, objectKeys, warmup, iters, getSamplesUs);
    report = SummarizeLatency(getSamplesUs);
}

void RunSameNodeGetRpcReductionSection(MetaAffinityStFixture *fixture, const std::string &payload, size_t payloadBytes,
                                       size_t warmup, size_t iters, LatencyReport &colocatedImmediateGet,
                                       LatencyReport &crossWorkerColdGet, LatencyReport &crossWorkerImmediateGet)
{
    const size_t slotSize = warmup + iters;
    std::vector<std::string> metaW0Keys;
    std::vector<std::string> metaW1Keys;
    fixture->GetHashKeysForMetaOwner(0, slotSize, metaW0Keys);
    fixture->GetHashKeysForMetaOwner(1, slotSize * 2, metaW1Keys);
    const auto crossWorkerColdKeys = SliceObjectKeys(metaW1Keys, 0, slotSize);
    const auto crossWorkerImmediateKeys = SliceObjectKeys(metaW1Keys, 1, slotSize);

    std::shared_ptr<ObjectClient> client0;
    fixture->InitTestClient(0, client0);

    std::vector<PutWritePerfSample> colocatedSamples;
    MeasurePutThenImmediateGets(fixture, client0, metaW0Keys, payload, 0, warmup, iters, false, colocatedSamples);
    std::vector<double> colocatedGetSamples;
    colocatedGetSamples.reserve(colocatedSamples.size());
    for (const auto &sample : colocatedSamples) {
        colocatedGetSamples.push_back(sample.getUs);
    }
    colocatedImmediateGet = SummarizeLatency(colocatedGetSamples);
    PrintMetaAffinityWritePerfJson("rpc_same_node_colocated_put_get_immediate", payloadBytes, warmup, iters,
                                   LatencyReport{}, colocatedImmediateGet, LatencyReport{}, 0);

    MeasureClient0ColdGetAfterGatewayReplicateKeys(fixture, payload, crossWorkerColdKeys, warmup, iters,
                                                   crossWorkerColdGet);
    PrintMetaAffinityWritePerfJson("rpc_same_node_cross_worker_cold_get_after_replicate", payloadBytes, warmup, iters,
                                   LatencyReport{}, crossWorkerColdGet, LatencyReport{}, 0);

    std::vector<PutWritePerfSample> crossWorkerImmediateSamples;
    MeasurePutThenImmediateGets(fixture, client0, crossWorkerImmediateKeys, payload, 1, warmup, iters, false,
                                crossWorkerImmediateSamples);
    std::vector<double> crossWorkerImmediateGetSamples;
    crossWorkerImmediateGetSamples.reserve(crossWorkerImmediateSamples.size());
    for (const auto &sample : crossWorkerImmediateSamples) {
        crossWorkerImmediateGetSamples.push_back(sample.getUs);
    }
    crossWorkerImmediateGet = SummarizeLatency(crossWorkerImmediateGetSamples);
    PrintMetaAffinityWritePerfJson("rpc_same_node_cross_worker_put_get_immediate", payloadBytes, warmup, iters,
                                   LatencyReport{}, crossWorkerImmediateGet, LatencyReport{}, 0);
}

void RunCrossNodeGetRpcReductionSection(MetaAffinityStFixture *fixture, const std::string &payload, size_t payloadBytes,
                                        size_t warmup, size_t iters, LatencyReport &gatewayColdGet,
                                        LatencyReport &directColdGet, LatencyReport &remoteImmediateGet,
                                        LatencyReport &remoteDirectColdGet, LatencyReport &remoteGatewayColdGet)
{
    const size_t slotSize = warmup + iters;
    std::vector<std::string> hashKeyPool;
    std::vector<std::string> routeKeyPool;
    fixture->GetHashKeysForMetaOwner(1, slotSize * 2, hashKeyPool);
    fixture->GetClientRouteKeysForMetaOwner(1, slotSize * 3, routeKeyPool);

    const auto gatewayW0Keys = SliceObjectKeys(hashKeyPool, 0, slotSize);
    const auto directW0Keys = SliceObjectKeys(routeKeyPool, 0, slotSize);
    const auto remoteImmediateKeys = SliceObjectKeys(routeKeyPool, 1, slotSize);
    const auto remoteDirectKeys = SliceObjectKeys(routeKeyPool, 2, slotSize);
    const auto remoteGatewayKeys = SliceObjectKeys(hashKeyPool, 1, slotSize);

    MeasureClient0ColdGetAfterGatewayReplicateKeys(fixture, payload, gatewayW0Keys, warmup, iters, gatewayColdGet);
    PrintMetaAffinityWritePerfJson("rpc_cross_node_w0_cold_get_after_gateway_replicate", payloadBytes, warmup, iters,
                                   LatencyReport{}, gatewayColdGet, LatencyReport{}, 0);

    MeasureClient0ColdGetAfterDirectRemoteWriteKeys(fixture, payload, directW0Keys, warmup, iters, directColdGet);
    PrintMetaAffinityWritePerfJson("rpc_cross_node_w0_cold_get_after_direct_remote_write", payloadBytes, warmup, iters,
                                   LatencyReport{}, directColdGet, LatencyReport{}, 0);

    MeasureRemotePutGetImmediateDirectKeys(fixture, payload, remoteImmediateKeys, warmup, iters, remoteImmediateGet);
    PrintMetaAffinityWritePerfJson("rpc_cross_node_remote_put_get_immediate_direct", payloadBytes, warmup, iters,
                                   LatencyReport{}, remoteImmediateGet, LatencyReport{}, 0);

    MeasureRemoteColdGetAfterDirectWriteKeys(fixture, payload, remoteDirectKeys, warmup, iters, remoteDirectColdGet);
    PrintMetaAffinityWritePerfJson("rpc_cross_node_remote_cold_get_after_direct_write", payloadBytes, warmup, iters,
                                   LatencyReport{}, remoteDirectColdGet, LatencyReport{}, 0);

    MeasureRemoteColdGetAfterGatewayReplicateKeys(fixture, payload, remoteGatewayKeys, warmup, iters,
                                                  remoteGatewayColdGet);
    PrintMetaAffinityWritePerfJson("rpc_cross_node_remote_cold_get_after_gateway_replicate", payloadBytes, warmup,
                                   iters, LatencyReport{}, remoteGatewayColdGet, LatencyReport{}, 0);
}

void RunCrossNodeGetBenchmarks(MetaAffinityStFixture *fixture, size_t payloadBytes, size_t warmup, size_t iters)
{
    const std::string payload = RandomData().GetRandomString(static_cast<int64_t>(payloadBytes));

    std::shared_ptr<ObjectClient> remoteReader;
    fixture->InitRemoteOnlyAffinityClient(remoteReader);

    std::shared_ptr<ObjectClient> directWriter;
    fixture->InitRemoteOnlyAffinityClient(directWriter);
    FLAGS_enable_meta_affinity_replicate = true;
    RunSteadyGetPerfScenario(fixture, directWriter, remoteReader, 1, payload, payloadBytes, warmup, iters,
                             "cross_node_remote_get_after_direct_write", false, true);
    RunImmediatePutGetPerfScenario(fixture, directWriter, 1, payload, payloadBytes, warmup, iters,
                                   "cross_node_remote_put_get_immediate_direct", false, true);

    std::shared_ptr<ObjectClient> gatewayWriter;
    fixture->InitTestClient(0, gatewayWriter);
    RunSteadyGetPerfScenario(fixture, gatewayWriter, remoteReader, 1, payload, payloadBytes, warmup, iters,
                             "cross_node_remote_get_after_gateway_replicate", true, false);
    RunGatewayPutRemoteImmediateGetScenario(fixture, gatewayWriter, remoteReader, 1, payload, payloadBytes, warmup,
                                            iters, "cross_node_remote_get_immediate_after_gateway_put");

    if (MetaAffinityWritePerfAssertEnabled()) {
        FLAGS_enable_meta_affinity_replicate = true;
    }
}

void ReadPerfSizeWarmupIters(size_t &payloadBytes, size_t &warmup, size_t &iters)
{
    const int defaultSize = ShouldRunGetRpcReductionPerf() ? 4096 : (256 * 1024);
    const int defaultWarmup = ShouldRunGetRpcReductionPerf() ? 15 : 20;
    const int defaultIters = ShouldRunGetRpcReductionPerf() ? 60 : 200;
    payloadBytes = static_cast<size_t>(EnvIntOrDefault("DS_META_AFFINITY_WRITE_PERF_SIZE", defaultSize));
    warmup = static_cast<size_t>(EnvIntOrDefault("DS_META_AFFINITY_WRITE_PERF_WARMUP", defaultWarmup));
    iters = static_cast<size_t>(EnvIntOrDefault("DS_META_AFFINITY_WRITE_PERF_ITERS", defaultIters));
}
}  // namespace

class MetaAffinityWritePerfTest : public MetaAffinityStFixture {};

TEST_F(MetaAffinityWritePerfTest, SameNodePutLatencyBenchmark)
{
    if (!MetaAffinityWritePerfEnabled() || !ShouldRunMetaAffinityWritePutPerf()) {
        GTEST_SKIP() << "Set DS_META_AFFINITY_WRITE_PERF=1 (and PERF_OP=put|all) for Put benchmark";
    }
    if (!ShouldRunMetaAffinityWritePerfLocalScenario()) {
        GTEST_SKIP() << "DS_META_AFFINITY_WRITE_PERF_MODE excludes local (same-node) scenario";
    }

    size_t payloadBytes = 0;
    size_t warmup = 0;
    size_t iters = 0;
    ReadPerfSizeWarmupIters(payloadBytes, warmup, iters);
    RunSameNodePutBenchmarks(this, payloadBytes, warmup, iters);
}

TEST_F(MetaAffinityWritePerfTest, SameNodeGetLatencyBenchmark)
{
    if (!MetaAffinityWritePerfEnabled() || !ShouldRunMetaAffinityWriteGetPerf()) {
        GTEST_SKIP() << "Set DS_META_AFFINITY_WRITE_PERF=1 (and PERF_OP=get|all) for Get benchmark";
    }
    if (!ShouldRunMetaAffinityWritePerfLocalScenario()) {
        GTEST_SKIP() << "DS_META_AFFINITY_WRITE_PERF_MODE excludes local (same-node) scenario";
    }

    size_t payloadBytes = 0;
    size_t warmup = 0;
    size_t iters = 0;
    ReadPerfSizeWarmupIters(payloadBytes, warmup, iters);
    RunSameNodeGetBenchmarks(this, payloadBytes, warmup, iters);
}

TEST_F(MetaAffinityWritePerfTest, CrossNodePutLatencyBenchmark)
{
    if (!MetaAffinityWritePerfEnabled() || !ShouldRunMetaAffinityWritePutPerf()) {
        GTEST_SKIP() << "Set DS_META_AFFINITY_WRITE_PERF=1 (and PERF_OP=put|all) for Put benchmark";
    }
    if (!ShouldRunMetaAffinityWritePerfRemoteScenario()) {
        GTEST_SKIP() << "DS_META_AFFINITY_WRITE_PERF_MODE excludes remote (cross-node) scenario";
    }

    size_t payloadBytes = 0;
    size_t warmup = 0;
    size_t iters = 0;
    ReadPerfSizeWarmupIters(payloadBytes, warmup, iters);
    RunCrossNodePutBenchmarks(this, payloadBytes, warmup, iters);
}

TEST_F(MetaAffinityWritePerfTest, CrossNodeGetLatencyBenchmark)
{
    if (!MetaAffinityWritePerfEnabled() || !ShouldRunMetaAffinityWriteGetPerf()) {
        GTEST_SKIP() << "Set DS_META_AFFINITY_WRITE_PERF=1 (and PERF_OP=get|all) for Get benchmark";
    }
    if (!ShouldRunMetaAffinityWritePerfRemoteScenario()) {
        GTEST_SKIP() << "DS_META_AFFINITY_WRITE_PERF_MODE excludes remote (cross-node) scenario";
    }

    size_t payloadBytes = 0;
    size_t warmup = 0;
    size_t iters = 0;
    ReadPerfSizeWarmupIters(payloadBytes, warmup, iters);
    RunCrossNodeGetBenchmarks(this, payloadBytes, warmup, iters);
}

TEST_F(MetaAffinityWritePerfTest, GetRpcReductionSameNodeBenchmark)
{
    if (!MetaAffinityWritePerfEnabled() || !ShouldRunGetRpcReductionPerf()) {
        GTEST_SKIP() << "Set DS_META_AFFINITY_WRITE_PERF=1 and DS_META_AFFINITY_WRITE_PERF_RPC=1";
    }
    if (!ShouldRunMetaAffinityWritePerfLocalScenario()) {
        GTEST_SKIP() << "DS_META_AFFINITY_WRITE_PERF_MODE excludes same-node scenario";
    }

    size_t payloadBytes = 0;
    size_t warmup = 0;
    size_t iters = 0;
    ReadPerfSizeWarmupIters(payloadBytes, warmup, iters);

    const std::string payload = RandomData().GetRandomString(static_cast<int64_t>(payloadBytes));
    LatencyReport colocatedImmediateGet;
    LatencyReport crossWorkerColdGet;
    LatencyReport crossWorkerImmediateGet;
    RunSameNodeGetRpcReductionSection(this, payload, payloadBytes, warmup, iters, colocatedImmediateGet,
                                      crossWorkerColdGet, crossWorkerImmediateGet);

    PrintMetaAffinityGetRpcReductionSummary(payloadBytes, warmup, iters, colocatedImmediateGet, crossWorkerColdGet,
                                            crossWorkerImmediateGet, LatencyReport{}, LatencyReport{}, LatencyReport{},
                                            LatencyReport{}, LatencyReport{});

    if (MetaAffinityWritePerfAssertEnabled()) {
        const double sameNodeImprovePct =
            crossWorkerColdGet.avgUs > 0
                ? (crossWorkerColdGet.avgUs - colocatedImmediateGet.avgUs) / crossWorkerColdGet.avgUs * 100.0
                : 0;
        EXPECT_GT(sameNodeImprovePct, 15.0)
            << "same-node colocated meta Put+Get should beat cross-worker cold Get RPC path";
        EXPECT_LT(colocatedImmediateGet.avgUs, crossWorkerColdGet.avgUs)
            << "same-node local meta should avoid cross-worker Get hops";
    }
}

TEST_F(MetaAffinityWritePerfTest, GetRpcReductionCrossNodeBenchmark)
{
    if (!MetaAffinityWritePerfEnabled() || !ShouldRunGetRpcReductionPerf()) {
        GTEST_SKIP() << "Set DS_META_AFFINITY_WRITE_PERF=1 and DS_META_AFFINITY_WRITE_PERF_RPC=1";
    }
    if (!ShouldRunMetaAffinityWritePerfRemoteScenario()) {
        GTEST_SKIP() << "DS_META_AFFINITY_WRITE_PERF_MODE excludes cross-node scenario";
    }

    size_t payloadBytes = 0;
    size_t warmup = 0;
    size_t iters = 0;
    ReadPerfSizeWarmupIters(payloadBytes, warmup, iters);

    const std::string payload = RandomData().GetRandomString(static_cast<int64_t>(payloadBytes));
    LatencyReport gatewayColdGet;
    LatencyReport directColdGet;
    LatencyReport remoteImmediateGet;
    LatencyReport remoteDirectColdGet;
    LatencyReport remoteGatewayColdGet;
    RunCrossNodeGetRpcReductionSection(this, payload, payloadBytes, warmup, iters, gatewayColdGet, directColdGet,
                                       remoteImmediateGet, remoteDirectColdGet, remoteGatewayColdGet);

    PrintMetaAffinityGetRpcReductionSummary(payloadBytes, warmup, iters, LatencyReport{}, LatencyReport{},
                                            LatencyReport{}, gatewayColdGet, directColdGet, remoteImmediateGet,
                                            remoteDirectColdGet, remoteGatewayColdGet);

    if (MetaAffinityWritePerfAssertEnabled()) {
        const double crossNodeImprovePct =
            gatewayColdGet.avgUs > 0 ? (gatewayColdGet.avgUs - directColdGet.avgUs) / gatewayColdGet.avgUs * 100.0 : 0;
        EXPECT_GT(crossNodeImprovePct, 15.0)
            << "cross-node direct meta-owner write should reduce W0 reader Get RPC latency";
        EXPECT_LT(remoteImmediateGet.avgUs, gatewayColdGet.avgUs)
            << "remote direct Put+Get should beat W0 cold Get after gateway replicate";
    }
}

}  // namespace st
}  // namespace datasystem
