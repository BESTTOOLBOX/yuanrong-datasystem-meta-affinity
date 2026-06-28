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
 * Description: Shared ST/perf fixtures for meta-affinity replicate and remote write.
 */
#ifndef DATASYSTEM_TESTS_ST_CLIENT_OBJECT_CACHE_META_AFFINITY_ST_FIXTURES_H
#define DATASYSTEM_TESTS_ST_CLIENT_OBJECT_CACHE_META_AFFINITY_ST_FIXTURES_H

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/object_cache/read_only_hash_ring_view.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/object_client.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/service_discovery.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "oc_client_common.h"

DS_DECLARE_bool(enable_meta_affinity_replicate);
DS_DECLARE_bool(enable_distributed_master);

namespace datasystem {
namespace st {

class MetaAffinityStFixture : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        opts.numOBS = 0;
        opts.enableSpill = false;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams += " -enable_meta_affinity_replicate=true -enable_data_replication=false ";

        std::string hostIp = "127.0.0.1";
        for (size_t i = 0; i < opts.numWorkers; i++) {
            HostPort hostPort(hostIp, GetFreePort());
            opts.workerConfigs.emplace_back(hostPort);
            std::string envName = "meta_affinity_host_id_env" + std::to_string(i);
            std::string envVal = "meta_affinity_host_id" + std::to_string(i);
            ASSERT_EQ(setenv(envName.c_str(), envVal.c_str(), 1), 0);
            opts.workerSpecifyGflagParams[i] = FormatString("-host_id_env_name=%s", envName);
        }
    }

    void GetHashKeyForMetaOwner(uint32_t workerIndex, std::string &objectKey)
    {
        objectKey = GetObjectKeyHashToWorker(db_.get(), workerIndex);
    }

    void GetHashKeysForMetaOwner(uint32_t workerIndex, size_t keyCount, std::vector<std::string> &objectKeys)
    {
        GetObjectKeysHashToWorker(db_.get(), workerIndex, keyCount, objectKeys);
    }

    // Keys whose client-side MurmurHash3 route (ReadOnlyHashRingView) lands on workerIndex.
    void GetClientRouteKeyForMetaOwner(uint32_t workerIndex, std::string &objectKey)
    {
        std::vector<std::string> objectKeys;
        GetClientRouteKeysForMetaOwner(workerIndex, 1, objectKeys);
        objectKey = objectKeys.front();
    }

    void GetClientRouteKeysForMetaOwner(uint32_t workerIndex, size_t keyCount, std::vector<std::string> &objectKeys)
    {
        HostPort targetAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, targetAddress));

        std::string ringValue;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", ringValue));
        object_cache::ReadOnlyHashRingView ringView;
        DS_ASSERT_OK(ringView.UpdateFromSerialized(ringValue, -1, nullptr));

        objectKeys.clear();
        constexpr char kClientRouteKeyPrefix[] = "a_key_hash_to_";
        constexpr uint64_t kMaxSearch = 10'000'000;
        for (uint64_t suffix = 0; objectKeys.size() < keyCount && suffix < kMaxSearch; ++suffix) {
            const std::string candidate = std::string(kClientRouteKeyPrefix) + std::to_string(suffix);
            HostPort metaAddress;
            if (ringView.GetMetaAddress(candidate, metaAddress).IsOk() && metaAddress == targetAddress) {
                objectKeys.push_back(candidate);
            }
        }
        ASSERT_EQ(objectKeys.size(), keyCount) << "failed to find enough client-route keys for worker " << workerIndex;
    }

    void InitRemoteOnlyAffinityClient(std::shared_ptr<ObjectClient> &client)
    {
        InitRemoteOnlyClientOnWorker(0, client);
    }

    void InitRemoteOnlyClientOnWorker(uint32_t workerIndex, std::shared_ptr<ObjectClient> &client)
    {
        const std::string envName = "meta_affinity_remote_client_env_w" + std::to_string(workerIndex);
        const std::string envVal = "meta_affinity_remote_client_host_w" + std::to_string(workerIndex);
        ASSERT_EQ(setenv(envName.c_str(), envVal.c_str(), 1), 0);
        ServiceDiscoveryOptions sdOpts;
        sdOpts.etcdAddress = cluster_->GetEtcdAddrs();
        sdOpts.hostIdEnvName = envName;
        sdOpts.affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE;
        auto serviceDiscovery = std::make_shared<ServiceDiscovery>(sdOpts);
        DS_ASSERT_OK(serviceDiscovery->Init());

        HostPort workerAddr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddr));
        ConnectOptions connectOptions;
        connectOptions.host = workerAddr.Host();
        connectOptions.port = workerAddr.Port();
        connectOptions.connectTimeoutMs = 60000;
        connectOptions.accessKey = accessKey_;
        connectOptions.secretKey = secretKey_;
        connectOptions.enableCrossNodeConnection = true;
        connectOptions.serviceDiscovery = serviceDiscovery;

        client = std::make_shared<ObjectClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void AssertMetaFetchHintOnWorker(const std::string &objectKey, uint32_t workerIndex)
    {
        master::QueryMetaInfoPb queryMeta;
        DS_ASSERT_OK(QueryMetaInfo(objectKey, queryMeta));
        const std::string &expected = workerIndex == 0 ? worker0Address_.ToString() : worker1Address_.ToString();
        EXPECT_EQ(queryMeta.address(), expected) << objectKey;
    }

    void AssertPrimaryIsImmediately(const std::string &objectKey, const std::string &expectedPrimary)
    {
        master::QueryMetaInfoPb queryMeta;
        DS_ASSERT_OK(QueryMetaInfo(objectKey, queryMeta));
        EXPECT_EQ(queryMeta.meta().primary_address(), expectedPrimary);
    }

    void AssertLocationsContainBothWorkers(const std::vector<std::string> &locations)
    {
        const std::string worker0Uuid = GetWorkerUuid(0, uuidMap_);
        const std::string worker1Uuid = GetWorkerUuid(1, uuidMap_);
        EXPECT_NE(std::find(locations.begin(), locations.end(), worker0Uuid), locations.end())
            << "origin worker should keep a local copy location";
        EXPECT_NE(std::find(locations.begin(), locations.end(), worker1Uuid), locations.end())
            << "meta owner should hold primary location";
    }

    bool IsPrimaryOnWorker(const std::string &objectKey, uint32_t workerIndex)
    {
        std::string primaryAddress;
        if (QueryPrimaryAddress(objectKey, primaryAddress).IsError()) {
            return false;
        }
        const std::string &expected = workerIndex == 0 ? worker0Address_.ToString() : worker1Address_.ToString();
        return primaryAddress == expected;
    }

    void WaitUntilPrimaryOnWorker(const std::string &objectKey, uint32_t workerIndex)
    {
        WaitUntilPrimaryIs(objectKey, workerIndex == 0 ? worker0Address_.ToString() : worker1Address_.ToString());
    }

    void DelayMetaAffinityReplicateOnWorker0(int sleepMs)
    {
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "MetaAffinityReplicate.BeforeMigrate",
                                               FormatString("sleep(%d)", sleepMs)));
    }

    void ClearMetaAffinityReplicateDelayOnWorker0()
    {
        DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, 0, "MetaAffinityReplicate.BeforeMigrate"));
    }

protected:
    void SetUp() override
    {
        OCClientCommon::SetUp();
        FLAGS_enable_meta_affinity_replicate = true;
        FLAGS_enable_distributed_master = true;
        db_ = InitTestEtcdInstance();
        SetWorkerHashInjection();
        std::this_thread::sleep_for(std::chrono::seconds(2));
        GetWorkerUuids(db_.get(), uuidMap_);
        InitMasterApis();
    }

    void InitMasterApis()
    {
        hostPort_.ParseString("127.0.0.1:" + std::to_string(GetFreePort()));
        akSkManager_ = std::make_shared<AkSkManager>();
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        RpcStubCacheMgr::Instance().Init(100);
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0Address_));
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker1Address_));
        worker0MasterApi_ = std::make_unique<worker::WorkerRemoteMasterOCApi>(worker0Address_, hostPort_, akSkManager_);
        DS_ASSERT_OK(worker0MasterApi_->Init());
        worker1MasterApi_ = std::make_unique<worker::WorkerRemoteMasterOCApi>(worker1Address_, hostPort_, akSkManager_);
        DS_ASSERT_OK(worker1MasterApi_->Init());
    }

    Status QueryMetaInfo(const std::string &objectKey, master::QueryMetaInfoPb &queryMeta)
    {
        master::QueryMetaReqPb queryReq;
        queryReq.add_ids(objectKey);
        queryReq.set_address(hostPort_.ToString());
        std::vector<RpcMessage> payloads;
        for (auto *api : { worker0MasterApi_.get(), worker1MasterApi_.get() }) {
            master::QueryMetaRspPb queryRsp;
            payloads.clear();
            Status status = api->QueryMeta(queryReq, 0, queryRsp, payloads);
            if (status.IsOk() && queryRsp.query_metas_size() > 0) {
                queryMeta = queryRsp.query_metas(0);
                return Status::OK();
            }
        }
        return Status(K_NOT_FOUND, "meta not found");
    }

    Status QueryPrimaryAddress(const std::string &objectKey, std::string &primaryAddress)
    {
        master::QueryMetaInfoPb queryMeta;
        RETURN_IF_NOT_OK(QueryMetaInfo(objectKey, queryMeta));
        primaryAddress = queryMeta.meta().primary_address();
        return Status::OK();
    }

    void WaitUntilPrimaryIs(const std::string &objectKey, const std::string &expectedPrimary)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        std::string primary;
        do {
            master::QueryMetaInfoPb queryMeta;
            if (QueryMetaInfo(objectKey, queryMeta).IsOk() && queryMeta.meta().primary_address() == expectedPrimary) {
                return;
            }
            if (QueryMetaInfo(objectKey, queryMeta).IsOk()) {
                primary = queryMeta.meta().primary_address();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        } while (std::chrono::steady_clock::now() < deadline);
        FAIL() << "primary did not become " << expectedPrimary << ", last=" << primary;
    }

    std::unique_ptr<EtcdStore> db_;
    HostPort hostPort_;
    HostPort worker0Address_;
    HostPort worker1Address_;
    std::unordered_map<HostPort, std::string> uuidMap_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::unique_ptr<worker::WorkerRemoteMasterOCApi> worker0MasterApi_;
    std::unique_ptr<worker::WorkerRemoteMasterOCApi> worker1MasterApi_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

}  // namespace st
}  // namespace datasystem

#endif  // DATASYSTEM_TESTS_ST_CLIENT_OBJECT_CACHE_META_AFFINITY_ST_FIXTURES_H
