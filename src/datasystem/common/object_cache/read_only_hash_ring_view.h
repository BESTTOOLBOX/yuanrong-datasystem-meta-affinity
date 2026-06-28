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
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_READ_ONLY_HASH_RING_VIEW_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_READ_ONLY_HASH_RING_VIEW_H

#include <cstdint>
#include <map>
#include <shared_mutex>
#include <string>

#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
enum class HashRingRefreshSource {
    NONE = 0,
    ETCD,
    WORKER,
};

class ReadOnlyHashRingView {
public:
    ~ReadOnlyHashRingView() = default;

    bool HasSnapshot() const;
    int64_t Version() const;
    bool HasScalingTask() const;
    bool IsWorkable() const;

    Status UpdateFromSerialized(const std::string &serializedRing, int64_t version, bool *versionChanged = nullptr);
    Status UpdateFromPb(const ClusterTopologyPb &ring, int64_t version, bool *versionChanged = nullptr);
    bool HasHealthyWorkerAtAddress(const HostPort &workerAddress) const;
    bool HasJoinableWorkerAtAddress(const HostPort &workerAddress) const;

    Status GetMetaAddress(const std::string &objectKey, HostPort &metaAddress) const;

private:
    void RebuildDerivedMapsLocked();

    mutable std::shared_mutex mutex_;
    ClusterTopologyPb ringInfo_;
    int64_t version_ = -1;
    std::map<uint32_t, std::string> tokenMap_;
    std::map<std::string, HostPort> workerUuid2AddrMap_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_READ_ONLY_HASH_RING_VIEW_H
