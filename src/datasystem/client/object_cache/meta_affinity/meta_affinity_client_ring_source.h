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
 * Description: Client hash ring lookup for metadata-affinity remote write (no local worker).
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_META_AFFINITY_CLIENT_RING_SOURCE_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_META_AFFINITY_CLIENT_RING_SOURCE_H

#include <memory>
#include <string>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/object_cache/read_only_hash_ring_view.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class MetaAffinityClientRingSource {
public:
    MetaAffinityClientRingSource(std::shared_ptr<IClientWorkerApi> gatewayWorkerApi, Signature *signature,
                                 int32_t requestTimeoutMs);
    ~MetaAffinityClientRingSource() = default;

    Status BootstrapRing();
    Status RefreshForRouteLookup();
    Status GetMetaAddress(const std::string &objectKey, HostPort &metaAddress);

private:
    Status EnsureInitialized();
    Status LoadFromEtcd();
    Status LoadFromWorker();

    std::shared_ptr<IClientWorkerApi> gatewayWorkerApi_;
    Signature *signature_;
    int32_t requestTimeoutMs_;
    std::shared_ptr<EtcdStore> etcdStore_;
    ReadOnlyHashRingView view_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_META_AFFINITY_CLIENT_RING_SOURCE_H
