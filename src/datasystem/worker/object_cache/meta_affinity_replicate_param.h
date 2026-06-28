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

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_META_AFFINITY_REPLICATE_PARAM_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_META_AFFINITY_REPLICATE_PARAM_H

#include <cstdint>
#include <string>
#include <vector>

namespace datasystem {
namespace object_cache {

struct MetaAffinityReplicateParam {
    std::string objectKey;
    uint64_t version{ 0 };
    uint32_t dataFormat{ 0 };
};

class MetaAffinityReplicateTask {
public:
    MetaAffinityReplicateTask() = default;
    explicit MetaAffinityReplicateTask(MetaAffinityReplicateParam param) : params_({ std::move(param) }) {}

    const std::vector<MetaAffinityReplicateParam> &GetParams() const { return params_; }

    const std::string &GetFirstObjectKey() const
    {
        static const std::string emptyString;
        return params_.empty() ? emptyString : params_.front().objectKey;
    }

private:
    std::vector<MetaAffinityReplicateParam> params_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_META_AFFINITY_REPLICATE_PARAM_H
