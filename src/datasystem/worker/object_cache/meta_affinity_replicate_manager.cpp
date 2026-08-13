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
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/util/format.h"

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
    LOG(INFO) << "MetaAffinityReplicateManager stopping, draining queued tasks";
    const auto deadline = std::chrono::steady_clock::now() + kStopDrainTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (GetQueueDepth() == 0 && inFlight_.load(std::memory_order_acquire) == 0) {
            break;
        }
        std::this_thread::sleep_for(kStopPollInterval);
    }
    if (GetQueueDepth() > 0 || inFlight_.load(std::memory_order_acquire) > 0) {
        LOG(WARNING) << FormatString("MetaAffinityReplicateManager drain timeout: queue_depth=%zu in_flight=%d",
                                     GetQueueDepth(), inFlight_.load(std::memory_order_acquire));
    }
    for (auto &thread : threadPool_) {
        thread.join();
    }
    threadPool_.clear();
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_META_AFFINITY_REPLICATE_QUEUE_DEPTH)).Set(0);
    LOG(INFO) << "MetaAffinityReplicateManager exit";
}

bool MetaAffinityReplicateManager::TryPopTask(int threadNum, MetaAffinityReplicateTask &task)
{
    std::lock_guard<std::mutex> lock(*locks_[threadNum]);
    if (queues_[threadNum].empty()) {
        return false;
    }
    if (running_.load(std::memory_order_acquire)) {
        const auto nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
        const auto quietNs = std::chrono::duration_cast<std::chrono::nanoseconds>(kForegroundQuietPeriod).count();
        if (nowNs - lastEnqueueNs_.load(std::memory_order_acquire) < quietNs) {
            return false;
        }
    }
    task = std::move(queues_[threadNum].front());
    queues_[threadNum].pop_front();
    return true;
}

Status MetaAffinityReplicateManager::Execute(int threadNum)
{
    LOG(INFO) << "MetaAffinityReplicateManager start execute, threadNum: " << threadNum;
    MetaAffinityReplicateTask task;
    while (running_.load(std::memory_order_acquire) || GetQueueDepth() > 0) {
        if (!TryPopTask(threadNum, task)) {
            if (!running_.load(std::memory_order_acquire) && GetQueueDepth() == 0) {
                break;
            }
            std::this_thread::sleep_for(kStopPollInterval);
            continue;
        }
        metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_META_AFFINITY_REPLICATE_QUEUE_DEPTH))
            .Set(static_cast<int64_t>(GetQueueDepth()));
        if (replicateFunc_) {
            inFlight_.fetch_add(1, std::memory_order_acq_rel);
            replicateFunc_(std::move(task));
            inFlight_.fetch_sub(1, std::memory_order_acq_rel);
        }
    }
    return Status::OK();
}

size_t MetaAffinityReplicateManager::ObjectKey2QueueIndex(const std::string &objectKey) const
{
    return std::hash<std::string>{}(objectKey) % kQueueNum;
}

size_t MetaAffinityReplicateManager::GetQueueDepth() const
{
    size_t depth = 0;
    for (size_t i = 0; i < kQueueNum; ++i) {
        std::lock_guard<std::mutex> lock(*locks_[i]);
        depth += queues_[i].size();
    }
    return depth;
}

int MetaAffinityReplicateManager::GetInFlightCount() const
{
    return inFlight_.load(std::memory_order_acquire);
}

Status MetaAffinityReplicateManager::AddTask(MetaAffinityReplicateTask &&task)
{
    if (!running_.load(std::memory_order_acquire)) {
        return Status(K_NOT_READY, "MetaAffinityReplicateManager is not running");
    }
    const auto index = ObjectKey2QueueIndex(task.GetFirstObjectKey());
    {
        std::lock_guard<std::mutex> lock(*locks_[index]);
        // Publish the quiet-period timestamp under the same lock that makes the
        // task visible, so the executor cannot observe the task with a stale
        // timestamp and start migration before the foreground grace period.
        lastEnqueueNs_.store(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_release);
        queues_[index].emplace_back(std::move(task));
    }
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_META_AFFINITY_REPLICATE_QUEUE_DEPTH))
        .Set(static_cast<int64_t>(GetQueueDepth()));
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
