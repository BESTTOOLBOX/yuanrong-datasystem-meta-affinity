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
 * Description: Read-only hash ring snapshot for client-side route lookup.
 */
#include "datasystem/common/object_cache/read_only_hash_ring_view.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"

DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_string(master_address);

namespace datasystem {
namespace object_cache {
namespace {
bool IsCentralizedMaster()
{
    return !FLAGS_enable_distributed_master;
}

Status ResolvePrimaryWorkerAddr(const std::map<uint32_t, std::string> &tokenMap, uint32_t keyHash,
                                std::string &workerAddr)
{
    if (tokenMap.empty()) {
        RETURN_STATUS(K_NOT_READY, "Hash ring token map is empty");
    }
    auto iter = tokenMap.upper_bound(keyHash);
    if (iter != tokenMap.end()) {
        workerAddr = iter->second;
        return Status::OK();
    }
    workerAddr = tokenMap.begin()->second;
    return Status::OK();
}

bool IsHealthyWorkerState(int state)
{
    return state == MembershipPb::ACTIVE || state == MembershipPb::LEAVING;
}

bool IsJoinableWorkerState(int state)
{
    return state == MembershipPb::ACTIVE || state == MembershipPb::LEAVING || state == MembershipPb::JOINING;
}

const MembershipPb *FindWorkerPbAtAddress(const ClusterTopologyPb &ringInfo,
                                          const std::map<std::string, HostPort> & /*workerUuid2AddrMap*/,
                                          const HostPort &workerAddress)
{
    const auto exact = ringInfo.members().find(workerAddress.ToString());
    if (exact != ringInfo.members().end()) {
        return &exact->second;
    }
    for (const auto &kv : ringInfo.members()) {
        HostPort mappedAddress;
        if (!mappedAddress.ParseString(kv.first).IsOk()) {
            continue;
        }
        if (mappedAddress == workerAddress) {
            return &kv.second;
        }
    }
    return nullptr;
}
}  // namespace

bool ReadOnlyHashRingView::HasSnapshot() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return version_ >= 0;
}

int64_t ReadOnlyHashRingView::Version() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return version_;
}

bool ReadOnlyHashRingView::HasScalingTask() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return false;
}

bool ReadOnlyHashRingView::IsWorkable() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return !tokenMap_.empty();
}

Status ReadOnlyHashRingView::UpdateFromSerialized(const std::string &serializedRing, int64_t version,
                                                  bool *versionChanged)
{
    ClusterTopologyPb ring;
    if (!ring.ParseFromString(serializedRing)) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Failed to parse hash ring snapshot");
    }
    return UpdateFromPb(ring, version, versionChanged);
}

Status ReadOnlyHashRingView::UpdateFromPb(const ClusterTopologyPb &ring, int64_t version, bool *versionChanged)
{
    std::unique_lock<std::shared_mutex> lock(mutex_);
    const int64_t previousVersion = version_;
    if (version_ >= 0 && version < 0) {
        if (versionChanged != nullptr) {
            *versionChanged = false;
        }
        return Status::OK();
    }
    if (version_ >= 0 && version >= 0 && version < version_) {
        if (versionChanged != nullptr) {
            *versionChanged = false;
        }
        return Status::OK();
    }
    if (version_ >= 0 && version >= 0 && version == version_
        && ringInfo_.SerializeAsString() == ring.SerializeAsString()) {
        if (versionChanged != nullptr) {
            *versionChanged = false;
        }
        return Status::OK();
    }
    if (version < 0 && version_ < 0 && ringInfo_.SerializeAsString() == ring.SerializeAsString()) {
        if (versionChanged != nullptr) {
            *versionChanged = false;
        }
        return Status::OK();
    }
    ringInfo_.CopyFrom(ring);
    version_ = version;
    RebuildDerivedMapsLocked();
    if (versionChanged != nullptr) {
        *versionChanged = previousVersion != version_ || previousVersion < 0;
    }
    return Status::OK();
}

Status ReadOnlyHashRingView::GetMetaAddress(const std::string &objectKey, HostPort &metaAddress) const
{
    if (IsCentralizedMaster()) {
        RETURN_IF_NOT_OK(metaAddress.ParseString(FLAGS_master_address));
        return Status::OK();
    }

    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (tokenMap_.empty()) {
        RETURN_STATUS(K_NOT_READY, "Hash ring snapshot is not ready");
    }

    const uint32_t keyHash = MurmurHash3_32(reinterpret_cast<const uint8_t *>(objectKey.data()), objectKey.size());
    std::string workerAddr;
    RETURN_IF_NOT_OK(ResolvePrimaryWorkerAddr(tokenMap_, keyHash, workerAddr));

    return metaAddress.ParseString(workerAddr);
}

bool ReadOnlyHashRingView::HasHealthyWorkerAtAddress(const HostPort &workerAddress) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const MembershipPb *workerPb = FindWorkerPbAtAddress(ringInfo_, workerUuid2AddrMap_, workerAddress);
    return workerPb != nullptr && IsHealthyWorkerState(workerPb->state());
}

bool ReadOnlyHashRingView::HasJoinableWorkerAtAddress(const HostPort &workerAddress) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const MembershipPb *workerPb = FindWorkerPbAtAddress(ringInfo_, workerUuid2AddrMap_, workerAddress);
    return workerPb != nullptr && IsJoinableWorkerState(workerPb->state());
}

void ReadOnlyHashRingView::RebuildDerivedMapsLocked()
{
    tokenMap_.clear();
    workerUuid2AddrMap_.clear();
    for (const auto &kv : ringInfo_.members()) {
        if (kv.second.state() != MembershipPb::ACTIVE && kv.second.state() != MembershipPb::LEAVING) {
            continue;
        }
        for (auto token : kv.second.tokens()) {
            tokenMap_.insert({ token, kv.first });
        }
    }
}
}  // namespace object_cache
}  // namespace datasystem
