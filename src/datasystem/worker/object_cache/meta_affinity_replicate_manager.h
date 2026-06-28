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

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_META_AFFINITY_REPLICATE_MANAGER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_META_AFFINITY_REPLICATE_MANAGER_H

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "datasystem/common/util/thread.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/meta_affinity_replicate_param.h"

namespace datasystem {
namespace object_cache {

class MetaAffinityReplicateManager {
public:
    MetaAffinityReplicateManager()
    {
        for (size_t i = 0; i < kQueueNum; ++i) {
            locks_.push_back(std::make_unique<std::mutex>());
            queues_.emplace_back();
        }
    }

    ~MetaAffinityReplicateManager() { Stop(); }

    MetaAffinityReplicateManager(const MetaAffinityReplicateManager &) = delete;
    MetaAffinityReplicateManager &operator=(const MetaAffinityReplicateManager &) = delete;

    Status Init(std::function<void(MetaAffinityReplicateTask &&task)> replicateFunc);

    void Stop();

    Status AddTask(MetaAffinityReplicateTask &&task);

private:
    static constexpr size_t kQueueNum = 4;

    Status Execute(int threadNum);
    size_t ObjectKey2QueueIndex(const std::string &objectKey) const;

    std::vector<std::unique_ptr<std::mutex>> locks_;
    std::vector<std::deque<MetaAffinityReplicateTask>> queues_;
    std::vector<Thread> threadPool_;
    std::atomic<bool> running_{ false };
    std::function<void(MetaAffinityReplicateTask &&task)> replicateFunc_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_META_AFFINITY_REPLICATE_MANAGER_H
