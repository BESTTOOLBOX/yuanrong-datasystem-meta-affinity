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

#include <mutex>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/kv_store.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(metastore_address);
DS_DECLARE_bool(enable_distributed_master);

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

Status EnsureRpcStubCache()
{
    static std::once_flag once;
    static Status initRc = Status::OK();
    std::call_once(once, []() { initRc = RpcStubCacheMgr::Instance().Init(2048); });
    return initRc;
}

Status GetWorkerOcStub(const HostPort &hostPort, std::shared_ptr<WorkerWorkerOCService_Stub> &stub)
{
    RETURN_IF_NOT_OK(EnsureRpcStubCache());
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(hostPort, StubType::WORKER_WORKER_OC_SVC, rpcStub));
    stub = std::dynamic_pointer_cast<WorkerWorkerOCService_Stub>(rpcStub);
    RETURN_RUNTIME_ERROR_IF_NULL(stub);
    return Status::OK();
}
}  // namespace

MetaAffinityClientRingSource::MetaAffinityClientRingSource(std::shared_ptr<IClientWorkerApi> gatewayWorkerApi,
                                                           Signature *signature, int32_t requestTimeoutMs)
    : gatewayWorkerApi_(std::move(gatewayWorkerApi)), signature_(signature), requestTimeoutMs_(requestTimeoutMs)
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
    RETURN_IF_NOT_OK(etcdStore_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX));
    return Status::OK();
}

Status MetaAffinityClientRingSource::LoadFromEtcd()
{
    RETURN_STATUS(K_NOT_READY, "Topology bootstrap uses worker GetHashRing");
}

Status MetaAffinityClientRingSource::LoadFromWorker()
{
    RETURN_RUNTIME_ERROR_IF_NULL(gatewayWorkerApi_);
    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    GetHashRingReqPb req;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));

    std::shared_ptr<WorkerWorkerOCService_Stub> stub;
    RETURN_IF_NOT_OK(GetWorkerOcStub(gatewayWorkerApi_->hostPort_, stub));
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    GetHashRingRspPb rsp;
    RETURN_IF_NOT_OK(stub->GetHashRing(opts, req, rsp));
    if (!rsp.hash_ring_changed()) {
        RETURN_STATUS(K_NOT_READY, "Worker returned no topology snapshot");
    }
    bool versionChanged = false;
    RETURN_IF_NOT_OK(view_.UpdateFromPb(rsp.hash_ring(), static_cast<int64_t>(rsp.version()), &versionChanged));
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
