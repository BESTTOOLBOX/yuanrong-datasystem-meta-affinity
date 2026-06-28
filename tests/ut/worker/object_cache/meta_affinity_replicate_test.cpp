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
#include "datasystem/worker/object_cache/meta_affinity_replicate_manager.h"

#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "datasystem/common/flags/flags.h"
#include "ut/common.h"

DS_DECLARE_bool(enable_meta_affinity_replicate);

namespace datasystem {
namespace ut {

using namespace object_cache;

class MetaAffinityReplicateTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        oldFlag_ = FLAGS_enable_meta_affinity_replicate;
    }

    void TearDown() override
    {
        FLAGS_enable_meta_affinity_replicate = oldFlag_;
        CommonTest::TearDown();
    }

protected:
    bool oldFlag_{ false };
};

TEST_F(MetaAffinityReplicateTest, ShouldScheduleWhenMetaOwnerDiffers)
{
    FLAGS_enable_meta_affinity_replicate = true;
    EXPECT_TRUE(ShouldScheduleMetaAffinityReplicate("127.0.0.1:10001", "127.0.0.1:10002"));
}

TEST_F(MetaAffinityReplicateTest, ShouldNotScheduleWhenSameNode)
{
    FLAGS_enable_meta_affinity_replicate = true;
    EXPECT_FALSE(ShouldScheduleMetaAffinityReplicate("127.0.0.1:10001", "127.0.0.1:10001"));
}

TEST_F(MetaAffinityReplicateTest, ShouldNotScheduleWhenFlagDisabled)
{
    FLAGS_enable_meta_affinity_replicate = false;
    EXPECT_FALSE(ShouldScheduleMetaAffinityReplicate("127.0.0.1:10001", "127.0.0.1:10002"));
}

TEST_F(MetaAffinityReplicateTest, ManagerExecutesQueuedTask)
{
    MetaAffinityReplicateManager manager;
    std::mutex mu;
    std::vector<std::string> executed;
    DS_ASSERT_OK(manager.Init([&](MetaAffinityReplicateTask &&task) {
        std::lock_guard<std::mutex> lock(mu);
        for (const auto &param : task.GetParams()) {
            executed.push_back(param.objectKey);
        }
    }));

    MetaAffinityReplicateParam param{ .objectKey = "obj-a", .version = 42, .dataFormat = 1 };
    DS_ASSERT_OK(manager.AddTask(MetaAffinityReplicateTask(std::move(param))));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        {
            std::lock_guard<std::mutex> lock(mu);
            if (!executed.empty()) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    {
        std::lock_guard<std::mutex> lock(mu);
        ASSERT_EQ(executed.size(), 1u);
        EXPECT_EQ(executed.front(), "obj-a");
    }
}

}  // namespace ut
}  // namespace datasystem
