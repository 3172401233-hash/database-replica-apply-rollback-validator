import unittest

from replica_rollback import (
    PartialApplication,
    ReplicaApplier,
    RetryRecord,
    Savepoint,
    Statement,
    Transaction,
)


def initial_tables():
    return {
        "orders": {
            1: {"id": 1, "order_no": "ORD-1"},
        },
        "invoices": {
            9: {"id": 9, "invoice_no": "INV-9"},
        },
        "inventory": {
            20: {"id": 20, "sku": "SKU-A", "qty": 5},
        },
    }


def unique_keys():
    return {
        "orders": ("order_no",),
        "invoices": ("invoice_no",),
        "inventory": ("sku",),
    }


def multi_table_transaction(savepoint=None):
    return Transaction(
        txid="TX-100",
        start_lsn=100,
        commit_lsn=104,
        savepoint=savepoint,
        statements=[
            Statement(
                lsn=101,
                table="orders",
                operation="INSERT",
                unique_key=("order_no", "ORD-2"),
                row={"id": 2, "order_no": "ORD-2"},
            ),
            Statement(
                lsn=102,
                table="invoices",
                operation="INSERT",
                unique_key=("invoice_no", "INV-2"),
                row={"id": 10, "invoice_no": "INV-2"},
            ),
            Statement(
                lsn=103,
                table="inventory",
                operation="INSERT",
                unique_key=("sku", "SKU-A"),
                row={"id": 21, "sku": "SKU-A", "qty": 1},
            ),
        ],
    )


class ReplicaRollbackTest(unittest.TestCase):
    def test_multi_table_conflict_rolls_back_all_writes_and_keeps_lsn_position(self):
        applier = ReplicaApplier(initial_tables(), unique_keys(), last_successful_lsn=99)

        result = applier.apply(
            multi_table_transaction(Savepoint("sp_after_orders", lsn=102))
        )

        self.assertEqual("CONFLICT", result.status)
        self.assertFalse(result.committed)
        self.assertEqual(applier.tables, initial_tables())
        self.assertEqual(100, result.next_apply_lsn)
        self.assertEqual(99, result.last_successful_lsn)
        self.assertEqual([101, 102, 103], result.rollback.statement_lsns)
        self.assertEqual(["orders", "invoices", "inventory"], result.rollback.tables)
        self.assertEqual("TRANSACTION", result.rollback.boundary)
        self.assertFalse(result.rollback.savepoint_covered_all)
        self.assertEqual("inventory", result.conflict.table)
        self.assertEqual("SKU-A", result.conflict.key_value)
        self.assertEqual(103, result.conflict.conflicting_lsn)
        self.assertEqual(1, result.retry_record.attempt)
        self.assertEqual(103, result.retry_record.last_lsn)

    def test_lost_savepoint_still_rolls_back_to_transaction_boundary(self):
        applier = ReplicaApplier(initial_tables(), unique_keys(), last_successful_lsn=99)
        lost_savepoint = Savepoint("sp_lost", lsn=102, available=False)

        result = applier.apply(multi_table_transaction(lost_savepoint))

        self.assertEqual("CONFLICT", result.status)
        self.assertEqual("TRANSACTION", result.rollback.boundary)
        self.assertFalse(result.rollback.savepoint_available)
        self.assertEqual(101, result.rollback.start_lsn)
        self.assertEqual(103, result.rollback.end_lsn)
        self.assertEqual(applier.tables, initial_tables())
        self.assertEqual(100, result.next_apply_lsn)
        self.assertIn("lost", result.rollback.note)

    def test_duplicate_replay_does_not_execute_after_commit(self):
        applier = ReplicaApplier(initial_tables(), unique_keys(), last_successful_lsn=99)
        transaction = Transaction(
            txid="TX-101",
            start_lsn=100,
            commit_lsn=101,
            statements=[
                Statement(
                    lsn=100,
                    table="orders",
                    operation="INSERT",
                    unique_key=("order_no", "ORD-3"),
                    row={"id": 3, "order_no": "ORD-3"},
                )
            ],
        )

        first = applier.apply(transaction)
        duplicate = applier.apply(transaction)

        self.assertEqual("COMMITTED", first.status)
        self.assertEqual("DUPLICATE", duplicate.status)
        self.assertIsNone(duplicate.retry_record)
        self.assertEqual(102, duplicate.next_apply_lsn)
        self.assertEqual(2, len(applier.tables["orders"]))
        self.assertEqual(1, len(applier.retry_records))

    def test_partial_application_is_removed_and_failed_transaction_blocks_later_lsn(self):
        applier = ReplicaApplier(initial_tables(), unique_keys(), last_successful_lsn=99)
        partial = PartialApplication(
            txid="TX-100",
            applied_statement_lsns=[101, 102],
            tables=["orders", "invoices"],
        )

        result = applier.apply(multi_table_transaction(), partial_application=partial)
        later_transaction = Transaction(
            txid="TX-101",
            start_lsn=200,
            commit_lsn=201,
            statements=[
                Statement(
                    lsn=200,
                    table="orders",
                    operation="INSERT",
                    unique_key=("order_no", "ORD-4"),
                    row={"id": 4, "order_no": "ORD-4"},
                )
            ],
        )

        blocked = applier.apply(later_transaction)

        self.assertEqual("CONFLICT", result.status)
        self.assertEqual([101, 102, 103], result.rollback.statement_lsns)
        self.assertEqual(applier.tables, initial_tables())
        self.assertEqual("BLOCKED", blocked.status)
        self.assertEqual(100, blocked.next_apply_lsn)
        self.assertIn("failed transaction", blocked.conflict_reason)

    def test_retry_history_keeps_failed_lsn_unmovable(self):
        retry_history = [
            RetryRecord(
                txid="TX-100",
                attempt=1,
                last_lsn=103,
                blocking_lsn=100,
                result="CONFLICT",
                reason="previous unique-key conflict",
            )
        ]
        applier = ReplicaApplier(
            initial_tables(),
            unique_keys(),
            last_successful_lsn=99,
            retry_records=retry_history,
        )
        later_transaction = Transaction(
            txid="TX-200",
            start_lsn=150,
            commit_lsn=151,
            statements=[
                Statement(
                    lsn=150,
                    table="orders",
                    operation="INSERT",
                    unique_key=("order_no", "ORD-5"),
                    row={"id": 5, "order_no": "ORD-5"},
                )
            ],
        )

        result = applier.apply(later_transaction)

        self.assertEqual("BLOCKED", result.status)
        self.assertEqual(100, result.next_apply_lsn)
        self.assertEqual(initial_tables(), applier.tables)

    def test_transaction_at_same_lsn_as_failed_transaction_is_blocked(self):
        retry_history = [
            RetryRecord(
                txid="TX-100",
                attempt=1,
                last_lsn=103,
                blocking_lsn=100,
                result="CONFLICT",
                reason="previous unique-key conflict",
            )
        ]
        applier = ReplicaApplier(
            initial_tables(),
            unique_keys(),
            last_successful_lsn=99,
            retry_records=retry_history,
        )
        same_lsn_transaction = Transaction(
            txid="TX-101",
            start_lsn=100,
            commit_lsn=104,
            statements=[
                Statement(
                    lsn=100,
                    table="orders",
                    operation="INSERT",
                    unique_key=("order_no", "ORD-7"),
                    row={"id": 7, "order_no": "ORD-7"},
                )
            ],
        )

        result = applier.apply(same_lsn_transaction)

        self.assertEqual("BLOCKED", result.status)
        self.assertEqual(100, result.next_apply_lsn)
        self.assertEqual(initial_tables(), applier.tables)

    def test_committed_transaction_from_retry_history_is_rejected_as_duplicate(self):
        retry_history = [
            RetryRecord(
                txid="TX-300",
                attempt=1,
                last_lsn=120,
                blocking_lsn=120,
                result="COMMITTED",
                reason="transaction committed atomically",
            )
        ]
        applier = ReplicaApplier(
            initial_tables(),
            unique_keys(),
            last_successful_lsn=120,
            retry_records=retry_history,
        )
        replayed = Transaction(
            txid="TX-300",
            start_lsn=100,
            commit_lsn=120,
            statements=[
                Statement(
                    lsn=100,
                    table="orders",
                    operation="INSERT",
                    unique_key=("order_no", "ORD-8"),
                    row={"id": 8, "order_no": "ORD-8"},
                )
            ],
        )

        result = applier.apply(replayed)

        self.assertEqual("DUPLICATE", result.status)
        self.assertIsNone(result.retry_record)
        self.assertEqual(initial_tables(), applier.tables)

    def test_successful_multi_table_transaction_is_atomic_and_advances_lsn(self):
        applier = ReplicaApplier(initial_tables(), unique_keys(), last_successful_lsn=99)
        transaction = Transaction(
            txid="TX-102",
            start_lsn=100,
            commit_lsn=103,
            statements=[
                Statement(
                    lsn=100,
                    table="orders",
                    operation="INSERT",
                    unique_key=("order_no", "ORD-6"),
                    row={"id": 6, "order_no": "ORD-6"},
                ),
                Statement(
                    lsn=101,
                    table="invoices",
                    operation="INSERT",
                    unique_key=("invoice_no", "INV-6"),
                    row={"id": 11, "invoice_no": "INV-6"},
                ),
                Statement(
                    lsn=102,
                    table="inventory",
                    operation="INSERT",
                    unique_key=("sku", "SKU-Z"),
                    row={"id": 22, "sku": "SKU-Z", "qty": 1},
                ),
            ],
        )

        result = applier.apply(transaction)

        self.assertEqual("COMMITTED", result.status)
        self.assertEqual(104, result.next_apply_lsn)
        self.assertEqual(103, result.last_successful_lsn)
        self.assertEqual("ORD-6", applier.tables["orders"][6]["order_no"])
        self.assertEqual("INV-6", applier.tables["invoices"][11]["invoice_no"])
        self.assertEqual("SKU-Z", applier.tables["inventory"][22]["sku"])


if __name__ == "__main__":
    unittest.main()
