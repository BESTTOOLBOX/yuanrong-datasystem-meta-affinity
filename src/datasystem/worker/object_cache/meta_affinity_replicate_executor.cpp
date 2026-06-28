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

#include "datasystem/worker/object_cache/meta_affinity_replicate_executor.h"

#include <algorithm>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/object_cache/data_migrator/data_migrator.h"

DS_DECLARE_bool(enable_meta_affinity_replicate);

namespace datasystem {
namespace object_cache {
namespace {

bool MetaMovingDone(const master::ReplacePrimaryRspPb &rsp)
{
    return !rsp.meta_is_moving() && rsp.info().empty();
}

Status ReplacePrimaryOnce(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::ReplacePrimaryReqPb &req,
                          master::ReplacePrimaryRspPb &rsp)
{
    INJECT_POINT("MetaAffinityReplicate.ReplacePrimary.skip", []() { return Status::OK(); });
    return api->ReplacePrimary(req, rsp);
}

Status ReplacePrimaryWithRetry(const MetaAffinityReplicateContext &ctx, const std::string &objectKey, uint64_t version,
                               const std::string &originPrimary, const std::string &newPrimary)
{
    std::shared_ptr<worker::WorkerMasterOCApi> api;
    RETURN_IF_NOT_OK(ctx.workerMasterApiManager->GetWorkerMasterApi(objectKey, api));
    if (api == nullptr) {
        return Status(K_RUNTIME_ERROR, "GetWorkerMasterApi failed for ReplacePrimary");
    }

    master::ReplacePrimaryReqPb req;
    master::ReplacePrimaryRspPb rsp;
    req.set_redirect(true);
    req.set_origin_primary_addr(originPrimary);
    req.set_new_primary_addr(newPrimary);
    req.set_remove_location(false);
    auto *info = req.add_object_infos();
    info->set_object_key(objectKey);
    info->set_version(version);

    constexpr int maxRetryCount = 3;
    Status status;
    for (int i = 0; i < maxRetryCount; ++i) {
        rsp.Clear();
        status = ReplacePrimaryOnce(api, req, rsp);
        if (status.IsError()) {
            continue;
        }
        if (MetaMovingDone(rsp)) {
            break;
        }
    }
    RETURN_IF_NOT_OK(status);
    if (std::find(rsp.success_ids().begin(), rsp.success_ids().end(), objectKey) != rsp.success_ids().end()) {
        return Status::OK();
    }
    if (std::find(rsp.expired_ids().begin(), rsp.expired_ids().end(), objectKey) != rsp.expired_ids().end()) {
        return Status::OK();
    }
    return Status(K_RUNTIME_ERROR, FormatString("ReplacePrimary failed for %s", objectKey));
}

void MarkLocalCopyOnOriginWorker(const MetaAffinityReplicateContext &ctx, const std::string &objectKey)
{
    std::shared_ptr<SafeObjType> entry;
    if (ctx.objectTable->Get(objectKey, entry).IsError() || entry->WLock().IsError()) {
        return;
    }
    // Origin publish worker keeps data as a non-primary local copy after primary moves to meta owner.
    (*entry)->stateInfo.SetPrimaryCopy(false);
    entry->WUnlock();
}

}  // namespace

bool ShouldScheduleMetaAffinityReplicate(const std::string &localAddress, const std::string &metaWorkerAddress)
{
    if (!FLAGS_enable_meta_affinity_replicate) {
        return false;
    }
    return !localAddress.empty() && !metaWorkerAddress.empty() && localAddress != metaWorkerAddress;
}

Status ExecuteMetaAffinityReplicate(const MetaAffinityReplicateParam &param, const MetaAffinityReplicateContext &ctx)
{
    if (!FLAGS_enable_meta_affinity_replicate) {
        return Status::OK();
    }
    if (ctx.metadataRouteResolver == nullptr || ctx.membership == nullptr || ctx.endpointPolicy == nullptr
        || ctx.objectTable == nullptr || ctx.akSkManager == nullptr || ctx.workerMasterApiManager == nullptr) {
        return Status(K_INVALID, "MetaAffinityReplicateContext is incomplete");
    }

    HostPort metaWorker;
    RETURN_IF_NOT_OK(ctx.metadataRouteResolver->ResolveOwner(param.objectKey, metaWorker));
    const std::string metaWorkerAddr = metaWorker.ToString();
    const std::string localAddr = ctx.localAddress.ToString();
    if (!ShouldScheduleMetaAffinityReplicate(localAddr, metaWorkerAddr)) {
        return Status::OK();
    }

    INJECT_POINT("MetaAffinityReplicate.BeforeMigrate");
    INJECT_POINT("MetaAffinityReplicate.Migrate.skip", []() { return Status::OK(); });

    HostPort localAddress = ctx.localAddress;
    DataMigrator migrator(MigrateType::SCALE_DOWN, *ctx.metadataRouteResolver, *ctx.membership, *ctx.endpointPolicy,
                          ctx.exitRequested, localAddress, ctx.akSkManager, ctx.objectTable);
    migrator.Init();
    auto future = migrator.MigrateToTargetNode({ param.objectKey }, metaWorker, nullptr, false, 0, false);
    const auto result = future.get();
    if (result.successIds.find(ImmutableString(param.objectKey)) == result.successIds.end()) {
        LOG(WARNING) << FormatString("[MetaAffinityReplicate] migrate %s to %s failed: %s", param.objectKey,
                                     metaWorkerAddr, result.status.ToString());
        return result.status.IsError() ? result.status
                                       : Status(K_RUNTIME_ERROR, "MetaAffinityReplicate migrate failed");
    }

    Status rc = ReplacePrimaryWithRetry(ctx, param.objectKey, param.version, localAddr, metaWorkerAddr);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[MetaAffinityReplicate] ReplacePrimary %s failed: %s", param.objectKey,
                                     rc.ToString());
        return rc;
    }

    MarkLocalCopyOnOriginWorker(ctx, param.objectKey);
    LOG(INFO) << FormatString("[MetaAffinityReplicate] object %s primary moved from %s to %s", param.objectKey,
                              localAddr, metaWorkerAddr);
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
