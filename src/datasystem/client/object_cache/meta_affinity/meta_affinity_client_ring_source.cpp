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
 * Description: Client hash ring bootstrap for metadata-affinity remote write.
 */
#include "datasystem/client/object_cache/meta_affinity/meta_affinity_client_ring_source.h"

#include <algorithm>

#include "datasystem/client/routing/routing_rpc_client.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/kv_store.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/object_posix.stub.rpc.pb.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(metastore_address);
DS_DECLARE_string(cluster_name);
DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_bool(use_brpc);

namespace datasystem {
namespace object_cache {
namespace {
std::string ResolveEtcdAddress()
{
    if (!FLAGS_metastore_address.empty()) {
        return FLAGS_metastore_address;
    }
    return FLAGS_etcd_address;
}

Status ApplyGetHashRingResponse(const GetHashRingRspPb &rsp, ReadOnlyHashRingView &view)
{
    if (!rsp.hash_ring_changed()) {
        RETURN_STATUS(K_NOT_READY, "Worker returned no topology snapshot");
    }
    CHECK_FAIL_RETURN_STATUS(rsp.has_hash_ring(), K_RUNTIME_ERROR,
                             "GetHashRing response is missing the changed hash ring");
    bool versionChanged = false;
    RETURN_IF_NOT_OK(view.UpdateFromPb(rsp.hash_ring(), static_cast<int64_t>(rsp.version()), &versionChanged));
    return Status::OK();
}
}  // namespace

MetaAffinityClientRingSource::MetaAffinityClientRingSource(std::shared_ptr<IClientWorkerApi> gatewayWorkerApi,
                                                           RpcCredential cred, Signature *signature,
                                                           int32_t requestTimeoutMs)
    : gatewayWorkerApi_(std::move(gatewayWorkerApi)),
      cred_(std::move(cred)),
      signature_(signature),
      requestTimeoutMs_(requestTimeoutMs)
{
}

Status MetaAffinityClientRingSource::GetMetaAddress(const std::string &objectKey, HostPort &metaAddress)
{
    return view_.GetMetaAddress(objectKey, metaAddress);
}

Status MetaAffinityClientRingSource::RefreshForRouteLookup()
{
    if (!FLAGS_enable_distributed_master) {
        return Status::OK();
    }
    if (!view_.HasSnapshot()) {
        return BootstrapRing();
    }
    if (view_.HasScalingTask()) {
        Status etcdRc = LoadFromEtcd();
        if (etcdRc.IsOk()) {
            return Status::OK();
        }
        return LoadFromWorker();
    }
    return Status::OK();
}

Status MetaAffinityClientRingSource::BootstrapRing()
{
    Status etcdRc = LoadFromEtcd();
    if (etcdRc.IsOk()) {
        return Status::OK();
    }
    Status workerRc = LoadFromWorker();
    if (workerRc.IsOk()) {
        return Status::OK();
    }
    return etcdRc.IsError() ? etcdRc : workerRc;
}

Status MetaAffinityClientRingSource::EnsureInitialized()
{
    if (etcdStore_ != nullptr) {
        return Status::OK();
    }
    const auto backendAddress = ResolveEtcdAddress();
    if (backendAddress.empty()) {
        RETURN_STATUS(K_INVALID, "etcd_address is empty for meta affinity hash ring bootstrap");
    }
    etcdStore_ = std::make_shared<EtcdStore>(backendAddress);
    RETURN_IF_NOT_OK(etcdStore_->Init());
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(FLAGS_cluster_name, keys));
    auto rc = etcdStore_->CreateTableWithExactPrefix(keys->TopologyTable(), keys->TopologyTable());
    RETURN_IF_NOT_OK_EXCEPT(rc, K_DUPLICATED);
    return Status::OK();
}

Status MetaAffinityClientRingSource::EnsureRoutingRpcClient()
{
    if (routingRpcClient_ != nullptr) {
        return Status::OK();
    }
    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    BrpcChannelConfig channelConfig;
    channelConfig.timeout_ms = requestTimeoutMs_;
    std::shared_ptr<Signature> signatureShared(signature_, [](Signature *) {});
    routingRpcClient_ = std::make_shared<client::RoutingRpcClient>(channelConfig, signatureShared);
    return routingRpcClient_->Init();
}

Status MetaAffinityClientRingSource::LoadFromEtcd()
{
    RETURN_IF_NOT_OK(EnsureInitialized());
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(FLAGS_cluster_name, keys));
    RangeSearchResult res;
    Status rc = etcdStore_->Get(keys->TopologyTable(), "", res);
    if (rc.GetCode() == K_NOT_FOUND) {
        RETURN_STATUS(K_NOT_READY, "Topology not found in etcd");
    }
    RETURN_IF_NOT_OK(rc);
    bool versionChanged = false;
    RETURN_IF_NOT_OK(view_.UpdateFromSerialized(res.value, res.modRevision, &versionChanged));
    return Status::OK();
}

Status MetaAffinityClientRingSource::LoadFromWorker()
{
    RETURN_RUNTIME_ERROR_IF_NULL(gatewayWorkerApi_);
    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    const uint64_t currentVersion = static_cast<uint64_t>(std::max<int64_t>(0, view_.Version()));
    GetHashRingReqPb req;
    req.set_version(currentVersion);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    GetHashRingRspPb rsp;

    if (FLAGS_use_brpc) {
        RETURN_IF_NOT_OK(EnsureRoutingRpcClient());
        RETURN_IF_NOT_OK(routingRpcClient_->GetHashRing(gatewayWorkerApi_->hostPort_, currentVersion, rsp));
    } else {
        auto channel = std::make_shared<RpcChannel>(gatewayWorkerApi_->hostPort_, cred_);
        WorkerOCService_Stub stub(channel, requestTimeoutMs_);
        RETURN_IF_NOT_OK(WithRpcDiag(stub.GetHashRing(opts, req, rsp), "GetHashRing", gatewayWorkerApi_->hostPort_));
    }
    return ApplyGetHashRingResponse(rsp, view_);
}
}  // namespace object_cache
}  // namespace datasystem
