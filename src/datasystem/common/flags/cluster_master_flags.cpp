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
 * Description: Cluster master flags shared by worker and client direct read routing.
 *
 * These flags were previously defined only in worker startup (worker_oc_server.cpp). Client direct read
 * common modules (ReadOnlyHashRingView / meta routing) need DS_DECLARE linkage without pulling worker
 * targets into the client library, so the definitions live here and are linked by both worker and client.
 */
#include "datasystem/common/flags/flags.h"

DS_DEFINE_bool(enable_distributed_master, true, "Whether to support distributed master, default is true.");
DS_DEFINE_string(master_address, "", "Address of ds master and the value cannot be empty.");
