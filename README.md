# database-replica-apply-rollback-validator

验证订阅端应用多表事务时的原子回滚、重复事务拒绝和 LSN 重放位置。

## 范围

本项目只使用 Python 标准库在内存中模拟逻辑复制应用过程，不连接 PostgreSQL、MySQL、SQLite 或任何其他数据库，也不发起网络连接。内存表仅用于表示目标副本的可见状态。

## 输入模型

- 源事务：`txid`、起始 LSN、提交 LSN、按 LSN 排序的多条语句和可选 savepoint。
- 目标表：表名到主键行的内存字典，代表应用前的副本状态。
- savepoint：名称、起始 LSN 和是否仍然可用。
- LSN：事务起始、语句、提交和当前成功水位线。
- 唯一键：每个目标表配置的唯一键列，用于检测 `INSERT`/`UPDATE` 冲突。
- 部分应用结果：已经写入的语句 LSN 前缀和涉及的目标表。
- 重试记录：事务 ID、尝试次数、最后处理 LSN、阻塞 LSN、结果和原因。

## 一致性规则

- 多表事务只有全部语句成功才提交；任一唯一键冲突都会撤销该事务已经写入的全部行。
- savepoint 只覆盖部分语句，或 savepoint 已丢失时，回滚边界提升为完整事务边界，避免跨表留下部分结果。
- 冲突事务不移动成功 LSN 水位线，`next_apply_lsn` 保持为该事务的起始 LSN。
- 已经成功提交的事务再次到达时返回 `DUPLICATE`，不执行任何语句。
- 未成功事务会保留阻塞 LSN；后续事务不能越过它，状态返回 `BLOCKED`。
- 每次冲突或成功应用都会生成一条 `RetryRecord`，重复拒绝不生成新的执行记录。

## 输出

`ReplicaApplier.apply()` 返回 `ApplyResult`，包含：

- 最终可见表状态。
- 回滚范围：边界、语句 LSN、涉及表、savepoint 是否可用以及覆盖说明。
- 下一条可应用 LSN 和最后成功 LSN。
- 唯一键冲突详情，包括冲突表、键、键值、冲突 LSN 和原因。
- 本次重试记录。

## 快速示例

```python
from replica_rollback import ReplicaApplier, Savepoint, Statement, Transaction

applier = ReplicaApplier(
    initial_tables={"inventory": {20: {"id": 20, "sku": "SKU-A"}}},
    unique_keys={"inventory": ("sku",)},
    last_successful_lsn=99,
)

transaction = Transaction(
    txid="TX-100",
    start_lsn=100,
    commit_lsn=102,
    savepoint=Savepoint("sp_orders", lsn=101),
    statements=[
        Statement(100, "orders", "INSERT", ("order_no", "ORD-2"),
                  {"id": 2, "order_no": "ORD-2"}),
        Statement(101, "inventory", "INSERT", ("sku", "SKU-A"),
                  {"id": 21, "sku": "SKU-A"}),
    ],
)

result = applier.apply(transaction)
print(result.status, result.next_apply_lsn, result.rollback.tables)
```

## 测试

运行：

```bash
python -m unittest
```

测试覆盖多表唯一键冲突、savepoint 丢失、重复回放、部分应用回滚、失败 LSN 阻塞和成功提交。

## Python 版本

代码使用 Python 3.8 可用的数据类和类型语法，不依赖第三方包。
