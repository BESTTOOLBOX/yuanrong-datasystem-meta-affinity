# Meta-Affinity Write + QueryMeta/Data Merge

## 1. 仓库用途

这是 `yuanrong-datasystem` 的私有开发镜像，用于保存和继续验证以下两部分工作：

1. PR1151 Meta-Affinity Write：尽量使对象的 primary data 与 Metadata Owner 位于同一 Worker。
2. `enableLocalCache=true` 且 Reader 有同节点 Worker 时，使用 TCP 或 UB 在一次 `QueryMeta` RPC 中返回 metadata + data，删除后续独立 `GetObjectRemote` RPC。

当前开发分支：

```text
feature/meta-affinity-querymeta-ub-read
```

原始 B0 对照提交：

```text
f21e8db8291a5d8061f87de6d266444f6f3d61ad
```

## 2. 主要改动

### 2.1 写侧：Meta-Affinity Write

- 无健康同节点 Worker 时，Writer 可以直接将写请求路由到 Metadata Owner，避免先写 Gateway Worker 再迁移。
- 有健康同节点 Worker 时，前台 `Put/Publish` 在本地写入完成后返回 ACK；后台再串行执行迁移。
- 后台迁移等待 10 ms 前台静默窗口，然后执行 `MigrateToTargetNode` 和 `ReplacePrimary(remove_location=false)`。
- primary 最终切换到 Metadata Owner，Writer-local Worker 上的 origin copy 保留。
- Master 在多 location 中优先选择 primary。

写侧开关：

```text
-enable_meta_affinity_replicate=true
```

该开关默认为 `false`，回滚时可关闭。

### 2.2 读侧：QueryMeta/Data Merge

基线路径：

```text
Reader-local Worker -> QueryMeta -> Metadata Owner
Reader-local Worker <- metadata  <- Metadata Owner
Reader-local Worker -> GetObjectRemote -> Data Owner
Reader-local Worker <- data            <- Data Owner
```

优化路径：

```text
Reader-local Worker -> QueryMeta              -> Metadata Owner
Reader-local Worker <- metadata + object data <- Metadata Owner
```

具体改动：

- TCP：Metadata Owner 本地有数据时，直接随 `QueryMeta` 响应返回 data。
- UB：Reader-local Worker 为 `QueryMeta` 准备共享 UB receive buffer，Metadata Owner 将数据写入该 buffer，同一 RPC 返回 metadata 和传输完成状态。
- UB 路径复用 pooled receive buffer，不为合并路径额外做一次 payload copy。
- 已实测覆盖到 8 MiB，不使用 512 KiB 作为 UB 合并路径的上限。

读侧对照开关：

```text
-enable_query_meta_ub_merge=true   # 优化策略：QueryMeta 合并 metadata + data
-enable_query_meta_ub_merge=false  # 基线策略：QueryMeta + GetObjectRemote
```

OFF/ON 使用同一份代码、同一二进制、同一 UB receive-buffer pool，唯一变量是是否合并 RPC。

## 3. 目标场景

### 场景 1：Remote-only Writer

Writer Client 没有健康同节点 Worker。开启 Meta-Affinity Write 后，请求直接路由至 Metadata Owner，数据初始就写在 Metadata Owner。

### 场景 2：Local Writer 异步迁移

Writer Client 与 Writer-local Worker 同节点，`enableLocalCache=true`。前台 Put 返回后，Worker 在后台将数据迁移到 Metadata Owner 并切换 primary。性能目标是前台 Put 与基线基本持平。

### 场景 3：Local Reader 冷读

Writer 和 Reader 在不同节点，两端都有健康同节点 Worker，`enableLocalCache=true`。Reader-local Worker 本地 miss，Metadata Owner 已经持有 metadata + data。

- Merge OFF：`QueryMeta` 后再执行 `GetObjectRemote`。
- Merge ON：一次 `QueryMeta` 返回 metadata + data。
- 分别测试单 Reader 和 5 Reader 并发冷读。

### 场景 4：Ring Lookup 稳态性

验证 client 使用本地 ring 计算 Metadata Owner；稳态 Put 期间不应每次重载 etcd ring，也不应每次请求 Gateway `GetClusterState`。

## 4. 当前性能结果

所有时延为算术平均值；括号为 4 轮均值的最小值—最大值。负百分比表示优化更快。

### 4.1 TCP 双机，无 netem

两台 aarch64 服务器，真实 BRPC/TCP，`enable_urma=false`，双向 ping RTT 约 0.12 ms。

#### 前台 Put

| Payload | 基线策略 | 优化策略 | 变化 |
|---:|---:|---:|---:|
| 4 KiB | 0.506 ms | 0.508 ms | +0.4% |
| 256 KiB | 0.804 ms | 0.824 ms | +2.5% |
| 512 KiB | 0.807 ms | 0.810 ms | +0.3% |

#### 单 Reader 冷读

| Payload | Merge OFF | Merge ON | 绝对节省 | 降幅 |
|---:|---:|---:|---:|---:|
| 4 KiB | 0.915 ms | 0.630 ms | 0.285 ms | 31.2% |
| 256 KiB | 1.267 ms | 0.990 ms | 0.276 ms | 21.8% |
| 512 KiB | 1.464 ms | 1.144 ms | 0.320 ms | 21.9% |

#### 5 Reader 并发冷读

| Payload | Merge OFF | Merge ON | 绝对节省 | 降幅 |
|---:|---:|---:|---:|---:|
| 4 KiB | 1.101 ms | 0.794 ms | 0.307 ms | 27.9% |
| 256 KiB | 1.533 ms | 1.201 ms | 0.332 ms | 21.7% |
| 512 KiB | 1.925 ms | 1.522 ms | 0.403 ms | 20.9% |

### 4.2 UB 单机 URMA Mock，无 netem

单台 aarch64 服务器、7 个 Worker，URMA Mock 数据面为 SHM+UDS。单 Reader 和 5 Reader 的所有 payload 均使用同一二进制做 Merge OFF/ON 对照。

#### 前台 Put

| Payload | 基线策略 | 优化策略 | 变化 |
|---:|---:|---:|---:|
| 4 KiB | 1.766 ms | 1.770 ms | +0.2% |
| 256 KiB | 1.980 ms | 1.961 ms | -1.0% |
| 512 KiB | 2.229 ms | 2.297 ms | +3.0% |
| 1 MiB | 2.385 ms | 2.374 ms | -0.5% |

#### 单 Reader 冷读

| Payload | Merge OFF | Merge ON | 绝对节省 | 降幅 |
|---:|---:|---:|---:|---:|
| 4 KiB | 1.411 ms | 1.044 ms | 0.367 ms | 26.0% |
| 256 KiB | 1.476 ms | 1.097 ms | 0.379 ms | 25.7% |
| 512 KiB | 1.512 ms | 1.128 ms | 0.384 ms | 25.4% |
| 1 MiB | 1.576 ms | 1.215 ms | 0.362 ms | 22.9% |
| 8 MiB | 2.689 ms | 2.348 ms | 0.341 ms | 12.7% |

#### 5 Reader 并发冷读

| Payload | Merge OFF | Merge ON | 绝对节省 | 降幅 |
|---:|---:|---:|---:|---:|
| 4 KiB | 1.863 ms | 1.428 ms | 0.436 ms | 23.4% |
| 256 KiB | 1.951 ms | 1.525 ms | 0.425 ms | 21.8% |
| 512 KiB | 2.038 ms | 1.600 ms | 0.439 ms | 21.5% |
| 1 MiB | 2.162 ms | 1.729 ms | 0.433 ms | 20.0% |
| 8 MiB | 3.897 ms | 3.417 ms | 0.480 ms | 12.3% |

## 5. 构建

### 5.1 普通 TCP

```bash
bash build.sh -P off -X off -M off -U off -j 8
```

### 5.2 真实 URMA/UB

```bash
tests/benchmark/meta_affinity_real_cluster/scripts/build_product_real_urma.sh -j 8
tests/benchmark/meta_affinity_real_cluster/scripts/build_benchmark.sh
```

`build_product_real_urma.sh` 明确使用：

```text
-M on   # 构建真实 URMA
-U off  # 关闭 URMA Mock
```

URMA Mock 只用于单机可重复验证，真实服务器跨机实验应使用真实 URMA。

## 6. 实验入口

真实集群实验手册：

- [tests/benchmark/meta_affinity_real_cluster/EXPERIMENT_GUIDE_CN.md](tests/benchmark/meta_affinity_real_cluster/EXPERIMENT_GUIDE_CN.md)
- [tests/benchmark/meta_affinity_real_cluster/README.md](tests/benchmark/meta_affinity_real_cluster/README.md)

每台 Client 机器使用相同代码和相同 benchmark 二进制，仅通过运行时 `task` 分配 Writer 或 Reader 角色。真实 5 Reader 实验要求 5 个 Reader Client 同时运行，并且每个 Reader 有独立的同节点 Worker。

## 7. 对照原则

- 所有时延使用算术平均值，不使用 p50 或中位数。
- 每个策略、每个 payload 执行 4 轮，每轮 `20 warmup + 200 measured`。
- 5 Reader 每轮共有 `5 x 200 = 1,000` 次计时 Get。
- Merge OFF/ON 必须使用同一代码、同一二进制、同一 payload、同一 key manifest 和同一资源配置。
- Merge OFF 必须观察到 `GetObjectRemote > 0`；Merge ON 必须观察到 `GetObjectRemote = 0`。
- `netem` 只在明确需要模拟网络延迟时使用。本文中列出的最新 TCP 和 UB 结果都没有施加 netem。
- 不将本地 loopback/URMA Mock 数字当成真实跨机 UB 的绝对性能。真实上线结论应以真实多机 UB 结果为准。

## 8. 当前限制与后续

- 当前已完成“Metadata Owner 已有数据时一次 QueryMeta 返回 metadata + data”。
- 如果 Metadata Owner 没有数据，仍需根据 metadata location 到 Data Owner 取数。
- 读取负载均衡不在当前改动中；如果大量 Reader 集中访问同一 Metadata Owner，仍需后续设计副本选择或请求分流。

## 9. 安全与仓库卫生

- 不要向仓库提交 GitHub/GitCode token、SSH 密码、Access Key、Secret Key 或真实服务器凭据。
- `cluster.json` 中只保留脱敏示例；真实凭据通过环境变量或站点密钥管理提供。
- benchmark 原始结果、Worker 日志和机器信息应在上传前检查是否包含私有地址或凭据。
