# declarative-reconciler-go

一套声明式资源的控制器调和运行时：把资源的期望状态（Spec）持续调和成实际状态（Status）。
底层存储可替换（`Store` 接口），本地使用内存实现即可跑通全流程，不依赖任何外部服务。

## 架构

```
            ┌────────────┐  watch   ┌────────────┐  enqueue  ┌──────────┐
  写入 ───▶ │   Store    │ ───────▶ │ Controller │ ────────▶ │ WorkQueue│
            │ (可替换)   │ ◀─────── │            │           │ (去重/退避)│
            └────────────┘  状态写回 └────────────┘           └────┬─────┘
                  ▲                                                 │ Get
                  │            ┌────────────┐                       ▼
                  └─────────── │  Reconciler │ ◀────────────── worker 池（并发）
                    CAS 写回   │ (用户实现)  │
                               └────────────┘
```

- `reconcile/types.go`：`Resource` / `ObjectMeta` / `Condition` / `ResourceStatus` 等核心类型。
- `reconcile/store.go`：`Store` 接口与 `MemoryStore` 实现（CAS、代际推进、终结器感知的删除、watch）。
- `reconcile/queue.go`：去重、退避重试的并发工作队列。
- `reconcile/controller.go`：事件监听、worker 池、热循环抑制、状态写回冲突重试。
- `reconcile/schema.go`：多存储版本的注册、枢纽/辐条转换、默认值、必填校验、不可变字段。
- `reconcile/cache.go`：多控制器共享的读取缓存与回退策略。
- `reconcile/finalizer.go`：带检查点的删除清理，支持崩溃恢复。
- `reconcile/authz.go`：权限校验与审计。

## 调和语义

1. **触发**：资源的创建、更新（Spec 变化）、进入删除流程都会触发调和。
   同一对象的多次变更在队列中合并去重；同一时刻同一对象只被一个 worker 处理，
   处理期间到达的变更会在本轮结束后重新排队，不丢变更也不重复执行。
2. **幂等**：调和必须以 `(key, generation)` 为幂等键。同一版本被重复调和不得产生重复副作用
   （测试中以副作用计数器验证恰好一次）。
3. **代际（Generation）**：仅当 Spec 真正变化时存储才推进 `Generation`；
   元数据或状态写回不推进。调和完成后写回 `Status.ObservedGeneration = Generation`，
   表示该版本期望状态已被观测。
4. **热循环抑制**：控制器记录每个键最近入队的 `(generation, deleting)`，
   自身状态写回不推进代际，因此不会再次入队，不会形成调和热循环。
5. **状态写回**：`UpdateStatusWithRetry` 每次先读最新对象、应用变更、CAS 写回；
   遇到 `ErrConflict` 重新读取并重试（上限 8 次）。因为总是基于最新版本写入，
   既不覆盖其他写入者的更新，也不会把已生效的期望状态回退。
6. **删除流程**：删除带终结器的对象时，存储只设置 `DeletionTimestamp`；
   `Finalizer` 按顺序执行清理步骤，**每步生效后先把进度检查点持久化到对象状态**，
   全部完成后才移除终结器，对象随即被真正删除。进程在步骤之间崩溃后重启
   （控制器启动时会 `List` 全量入队），从检查点继续：已完成的步骤不重复执行，
   未生效的步骤重新执行（步骤实现必须幂等）。

## 队列参数

```go
QueueConfig{
    Concurrency: 8,          // worker 协程数；慢资源只占用自己的 worker，不阻塞其他资源
    MaxRetries:  5,          // 单键最大重试次数，超过后丢弃，避免持续失败拖垮整个队列
    Backoff: BackoffPolicy{
        BaseDelay: 10ms,     // 首次重试等待
        Factor:    2,        // 指数因子
        MaxDelay:  1s,       // 退避上限
    },
}
```

- 第 n 次重试的等待为 `BaseDelay * Factor^(n-1)`，封顶 `MaxDelay`。
- 处理成功调用 `Forget` 清零重试计数；失败调用 `Requeue` 按退避延迟重新入队。

## 版本兼容规则

- 每种资源（group/kind）可注册多个存储版本，其中**恰好一个枢纽（Hub）版本**（当前版本）。
- 读取旧版本对象时经 `Registry.ToHub` 转换到枢纽版本；`FromHub` 可转回任意已注册版本。
- 转换采用枢纽/辐条模型：每个旧版本实现 `ConvertToHub` / `ConvertFromHub`。
  **任意对象在版本间来回转换不得丢字段或改变语义**（测试做往返断言）。
- 转换只改 `APIVersion` 与 `Spec`，元数据与状态原样保留。
- 写入前依次执行：`Default`（默认值填充）→ `Validate`（必填校验），失败即拒绝写入。
- 更新时按枢纽版本的 `ImmutableFields` 校验：新旧对象各自转换到枢纽版本后逐字段比较，
  修改不可变字段返回 `ErrImmutable`，跨版本表示差异不会造成误判。

## 缓存回退策略

`Cache` 被多个控制器与执行单元共享：

1. 命中且未过期：直接返回副本；
2. 未命中或已过期（TTL）：回退到直接读取底层存储并刷新缓存；
3. 直接读取失败但有过期副本：降级返回过期副本（可用性优先），否则返回错误；
4. watch 事件驱动 `Invalidate`，保证不会长期读到旧值。

## 权限与审计

`SecureStore` 包装任意 `Store`：所有创建/更新/删除先经 `Authorizer` 判定，
越权写入返回 `ErrForbidden` 且不生效；每一次变更与拒绝都会写入 `AuditSink`
（时间、用户、动作、资源标识、是否允许、原因），可追溯。

## 测试

```sh
go test ./... -race -count=1
```

覆盖：并发调和与幂等去重、冲突重试不丢更新、版本往返转换、
删除清理崩溃恢复（单进程内模拟 + 控制器重启两种）、缓存过期与故障回退、
越权拒绝与审计。固定输入下结果可复现。
