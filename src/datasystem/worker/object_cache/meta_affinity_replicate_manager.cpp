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

#include "datasystem/worker/object_cache/meta_affinity_replicate_manager.h"

#include <chrono>
#include <thread>

#include "datasystem/common/log/log.h"

namespace datasystem {
namespace object_cache {

Status MetaAffinityReplicateManager::Init(std::function<void(MetaAffinityReplicateTask &&task)> replicateFunc)
{
    replicateFunc_ = std::move(replicateFunc);
    running_.store(true);
    for (size_t i = 0; i < kQueueNum; ++i) {
        threadPool_.emplace_back(Thread(&MetaAffinityReplicateManager::Execute, this, static_cast<int>(i)));
    }
    return Status::OK();
}

void MetaAffinityReplicateManager::Stop()
{
    if (!running_.exchange(false)) {
        return;
    }
    LOG(INFO) << "MetaAffinityReplicateManager exit";
    for (auto &thread : threadPool_) {
        thread.join();
    }
    threadPool_.clear();
}

Status MetaAffinityReplicateManager::Execute(int threadNum)
{
    LOG(INFO) << "MetaAffinityReplicateManager start execute, threadNum: " << threadNum;
    MetaAffinityReplicateTask task;
    while (running_.load()) {
        bool hasTask = false;
        {
            std::lock_guard<std::mutex> lock(*locks_[threadNum]);
            if (!queues_[threadNum].empty()) {
                task = std::move(queues_[threadNum].front());
                queues_[threadNum].pop_front();
                hasTask = true;
            }
        }
        if (!hasTask) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        if (replicateFunc_) {
            replicateFunc_(std::move(task));
        }
    }
    return Status::OK();
}

size_t MetaAffinityReplicateManager::ObjectKey2QueueIndex(const std::string &objectKey) const
{
    return std::hash<std::string>{}(objectKey) % kQueueNum;
}

Status MetaAffinityReplicateManager::AddTask(MetaAffinityReplicateTask &&task)
{
    if (!running_.load()) {
        return Status(K_NOT_READY, "MetaAffinityReplicateManager is not running");
    }
    const auto index = ObjectKey2QueueIndex(task.GetFirstObjectKey());
    std::lock_guard<std::mutex> lock(*locks_[index]);
    queues_[index].emplace_back(std::move(task));
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
