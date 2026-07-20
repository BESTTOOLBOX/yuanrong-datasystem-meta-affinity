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

#include "meta_affinity_st_fixtures.h"

namespace datasystem {
namespace st {

using MetaAffinityReplicateStTest = MetaAffinityStFixture;

TEST_F(MetaAffinityReplicateStTest, ColocatePrimaryWithMetaOwnerAndReadLocalCopy)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);

    std::string objectKey;
    GetHashKeyForMetaOwner(1, objectKey);
    const std::string payload = RandomData().GetRandomString(1024);

    CreateParam param;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client0->Create(objectKey, payload.size(), param, buffer));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(payload.data(), payload.size()));
    DS_ASSERT_OK(buffer->Seal({}));
    DS_ASSERT_OK(buffer->UnWLatch());

    // Meta owner is worker1 (hash injection); after async replicate primary must colocate with meta owner.
    WaitUntilPrimaryIs(objectKey, worker1Address_.ToString());

    master::QueryMetaInfoPb queryMeta;
    DS_ASSERT_OK(QueryMetaInfo(objectKey, queryMeta));
    EXPECT_EQ(queryMeta.meta().primary_address(), worker1Address_.ToString());
    // Remote fetch hint should prefer primary (meta owner), not the origin local copy.
    EXPECT_EQ(queryMeta.address(), worker1Address_.ToString());

    std::vector<ObjMetaInfo> objMetas;
    DS_ASSERT_OK(client0->GetObjMetaInfo("", { objectKey }, objMetas));
    ASSERT_EQ(objMetas.size(), 1u);
    EXPECT_EQ(objMetas[0].objSize, payload.size());
    AssertLocationsContainBothWorkers(objMetas[0].locations);

    // Worker0 still serves from its local copy when data is present locally.
    std::vector<Optional<Buffer>> worker0Buffers;
    DS_ASSERT_OK(client0->Get({ objectKey }, 0, worker0Buffers));
    ASSERT_EQ(worker0Buffers.size(), 1u);
    ASSERT_TRUE(worker0Buffers[0]);
    AssertBufferEqual(*worker0Buffers[0], payload);

    // Worker1 reads primary copy locally after colocation.
    std::vector<Optional<Buffer>> worker1Buffers;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, worker1Buffers));
    ASSERT_EQ(worker1Buffers.size(), 1u);
    ASSERT_TRUE(worker1Buffers[0]);
    AssertBufferEqual(*worker1Buffers[0], payload);

    // Invalidate local cache on worker0; next Get should pull from primary on worker1.
    DS_ASSERT_OK(worker0Buffers[0]->InvalidateBuffer());
    worker0Buffers.clear();
    DS_ASSERT_OK(client0->Get({ objectKey }, 0, worker0Buffers));
    ASSERT_EQ(worker0Buffers.size(), 1u);
    ASSERT_TRUE(worker0Buffers[0]);
    AssertBufferEqual(*worker0Buffers[0], payload);
}

TEST_F(MetaAffinityReplicateStTest, RemoteOnlyClientPutDirectlyOnMetaOwner)
{
    std::string objectKey;
    GetClientRouteKeyForMetaOwner(1, objectKey);
    const std::string payload = RandomData().GetRandomString(1024);

    std::shared_ptr<ObjectClient> remoteClient;
    InitRemoteOnlyAffinityClient(remoteClient);
    CreateParam param;
    DS_ASSERT_OK(
        remoteClient->Put(objectKey, reinterpret_cast<const uint8_t *>(payload.data()), payload.size(), param));

    // Remote-only client writes to hash-routed meta owner directly; no async replicate from gateway worker.
    AssertPrimaryIsImmediately(objectKey, worker1Address_.ToString());

    std::vector<ObjMetaInfo> objMetas;
    DS_ASSERT_OK(remoteClient->GetObjMetaInfo("", { objectKey }, objMetas));
    ASSERT_EQ(objMetas.size(), 1u);
    EXPECT_EQ(objMetas[0].objSize, payload.size());
    ASSERT_EQ(objMetas[0].locations.size(), 1u);
    const std::string worker1Location = worker1Address_.ToString();
    const std::string worker1Uuid = GetWorkerUuid(1, uuidMap_);
    EXPECT_TRUE(objMetas[0].locations[0] == worker1Location || objMetas[0].locations[0] == worker1Uuid);

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(remoteClient->Get({ objectKey }, 0, buffers));
    ASSERT_EQ(buffers.size(), 1u);
    ASSERT_TRUE(buffers[0]);
    AssertBufferEqual(*buffers[0], payload);
}

}  // namespace st
}  // namespace datasystem
