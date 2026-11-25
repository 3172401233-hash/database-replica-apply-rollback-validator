from typing import Dict, List, Optional, Tuple

from .models import (
    INSERT,
    ApplyResult,
    PartialApplication,
    RetryRecord,
    RollbackRange,
    Transaction,
    UndoEntry,
    UniqueKeyConflict,
    _Conflict,
    copied_retry_records,
    copied_tables,
)


COMMITTED = "COMMITTED"
CONFLICT = "CONFLICT"
DUPLICATE = "DUPLICATE"
BLOCKED = "BLOCKED"
STALE = "STALE"


class ReplicaApplier:
    """Apply logical replication transactions without using a database."""

    def __init__(
        self,
        initial_tables: Dict[str, Dict[object, Dict[str, object]]],
        unique_keys: Dict[str, Tuple[str, ...]],
        last_successful_lsn: int = 0,
        retry_records: Optional[List[RetryRecord]] = None,
        committed_txids: Optional[List[str]] = None,
    ):
        if last_successful_lsn < 0:
            raise ValueError("last successful LSN cannot be negative")
        self._tables = copied_tables(initial_tables)
        self._unique_keys = dict(unique_keys)
        self._last_successful_lsn = last_successful_lsn
        self._committed_txids = set(committed_txids or [])
        self._blocking_transactions: Dict[str, int] = {}
        self._retry_records = copied_retry_records(retry_records or [])
        for record in self._retry_records:
            if record.result == COMMITTED:
                self._committed_txids.add(record.txid)
            if record.result == CONFLICT:
                self._blocking_transactions[record.txid] = record.blocking_lsn

    @property
    def tables(self):
        return copied_tables(self._tables)

    @property
    def retry_records(self):
        return copied_retry_records(self._retry_records)

    @property
    def last_successful_lsn(self):
        return self._last_successful_lsn

    @property
    def next_apply_lsn(self):
        return self._last_successful_lsn + 1

    def apply(
        self,
        transaction: Transaction,
        partial_application: Optional[PartialApplication] = None,
    ) -> ApplyResult:
        duplicate_result = self._reject_if_not_applicable(transaction)
        if duplicate_result is not None:
            return duplicate_result

        attempt = self._next_attempt(transaction.txid)
        undo_entries: List[UndoEntry] = []
        start_index = 0

        if partial_application is not None:
            start_index = self._load_partial_application(transaction, partial_application)
            undo_entries.extend(self._load_partial_writes(transaction, start_index))

        try:
            for index in range(start_index, len(transaction.statements)):
                self._execute(transaction.statements[index], undo_entries)
        except _Conflict as error:
            return self._handle_conflict(
                transaction=transaction,
                undo_entries=undo_entries,
                conflict=error.conflict,
                attempt=attempt,
                partial_application=partial_application,
            )

        self._commit(transaction)
        retry_record = RetryRecord(
            txid=transaction.txid,
            attempt=attempt,
            last_lsn=transaction.commit_lsn,
            blocking_lsn=transaction.commit_lsn,
            result=COMMITTED,
            reason="transaction committed atomically",
        )
        self._retry_records.append(retry_record)
        return ApplyResult(
            status=COMMITTED,
            txid=transaction.txid,
            committed=True,
            visible_tables=self.tables,
            next_apply_lsn=transaction.commit_lsn + 1,
            last_successful_lsn=transaction.commit_lsn,
            partial_application=partial_application,
            rollback=None,
            conflict=None,
            conflict_reason=None,
            retry_record=retry_record,
        )

    def _reject_if_not_applicable(self, transaction):
        if transaction.txid in self._committed_txids:
            return self._rejection_result(
                transaction,
                DUPLICATE,
                "transaction %s has already committed and must not execute again"
                % transaction.txid,
            )

        for blocked_txid, blocked_lsn in self._blocking_transactions.items():
            if blocked_txid != transaction.txid and transaction.start_lsn >= blocked_lsn:
                return self._rejection_result(
                    transaction,
                    BLOCKED,
                    "LSN %s cannot pass failed transaction %s at LSN %s"
                    % (transaction.start_lsn, blocked_txid, blocked_lsn),
                )

        if transaction.start_lsn <= self._last_successful_lsn:
            return self._rejection_result(
                transaction,
                STALE,
                "transaction start LSN %s is not later than last successful LSN %s"
                % (transaction.start_lsn, self._last_successful_lsn),
            )
        return None

    def _rejection_result(self, transaction, status, reason):
        return ApplyResult(
            status=status,
            txid=transaction.txid,
            committed=False,
            visible_tables=self.tables,
            next_apply_lsn=self.next_apply_lsn,
            last_successful_lsn=self._last_successful_lsn,
            partial_application=None,
            rollback=None,
            conflict=None,
            conflict_reason=reason,
            retry_record=None,
        )

    def _next_attempt(self, txid):
        attempts = [record.attempt for record in self._retry_records if record.txid == txid]
        return (max(attempts) + 1) if attempts else 1

    def _load_partial_application(self, transaction, partial_application):
        if partial_application.txid != transaction.txid:
            raise ValueError("partial application belongs to another transaction")
        statement_lsns = [statement.lsn for statement in transaction.statements]
        partial_lsns = partial_application.applied_statement_lsns
        expected_prefix = statement_lsns[: len(partial_lsns)]
        if not partial_lsns or partial_lsns != expected_prefix:
            raise ValueError("partial application must be a non-empty statement prefix")

        partial_tables = {
            statement.table
            for statement in transaction.statements[: len(partial_lsns)]
        }
        if set(partial_application.tables) != partial_tables:
            raise ValueError("partial application tables do not match applied statements")
        return len(partial_lsns)

    def _load_partial_writes(self, transaction, prefix_length):
        undo_entries = []
        for statement in transaction.statements[:prefix_length]:
            table = self._tables.setdefault(statement.table, {})
            primary_key_value = statement.primary_key_value()
            table[primary_key_value] = dict(statement.row)
            undo_entries.append(
                UndoEntry(
                    statement_lsn=statement.lsn,
                    table=statement.table,
                    primary_key_value=primary_key_value,
                    old_row=None,
                )
            )
        return undo_entries

    def _execute(self, statement, undo_entries):
        table = self._tables.setdefault(statement.table, {})
        key_name, key_value = statement.unique_key
        self._check_unique_key(statement, table, key_name, key_value)
        primary_key_value = statement.primary_key_value()

        if statement.operation == INSERT:
            old_row = table.get(primary_key_value)
            if statement.row is None:
                raise ValueError("INSERT statement requires a row")
            table[primary_key_value] = dict(statement.row)
        elif statement.operation == UPDATE:
            if primary_key_value not in table:
                raise ValueError("cannot update missing row in table %s" % statement.table)
            old_row = dict(table[primary_key_value])
            if statement.row is None:
                raise ValueError("UPDATE statement requires a row")
            table[primary_key_value] = dict(statement.row)
        elif statement.operation == DELETE:
            if primary_key_value not in table:
                raise ValueError("cannot delete missing row in table %s" % statement.table)
            old_row = dict(table.pop(primary_key_value))
        else:
            raise ValueError("unsupported operation %s" % statement.operation)

        undo_entries.append(
            UndoEntry(
                statement_lsn=statement.lsn,
                table=statement.table,
                primary_key_value=primary_key_value,
                old_row=old_row,
            )
        )

    def _check_unique_key(self, statement, table, key_name, key_value):
        configured_keys = self._unique_keys.get(statement.table, ())
        if key_name not in configured_keys:
            raise ValueError(
                "unique key %s is not configured for table %s"
                % (key_name, statement.table)
            )
        for existing_primary_key, existing_row in table.items():
            if existing_row.get(key_name) == key_value:
                primary_key_value = statement.primary_key_value()
                if existing_primary_key != primary_key_value:
                    raise _Conflict(
                        UniqueKeyConflict(
                            table=statement.table,
                            key_name=key_name,
                            key_value=key_value,
                            conflicting_lsn=statement.lsn,
                            existing_primary_key=existing_primary_key,
                            reason=(
                                "unique key %s=%r on table %s conflicts with existing row %r"
                                % (key_name, key_value, statement.table, existing_primary_key)
                            ),
                        )
                    )

    def _handle_conflict(
        self,
        transaction,
        undo_entries,
        conflict,
        attempt,
        partial_application,
    ):
        rollback = self._rollback(transaction, undo_entries, conflict)
        self._blocking_transactions[transaction.txid] = transaction.start_lsn
        retry_record = RetryRecord(
            txid=transaction.txid,
            attempt=attempt,
            last_lsn=conflict.conflicting_lsn,
            blocking_lsn=transaction.start_lsn,
            result=CONFLICT,
            reason=conflict.reason,
        )
        self._retry_records.append(retry_record)
        return ApplyResult(
            status=CONFLICT,
            txid=transaction.txid,
            committed=False,
            visible_tables=self.tables,
            next_apply_lsn=transaction.start_lsn,
            last_successful_lsn=self._last_successful_lsn,
            partial_application=partial_application,
            rollback=rollback,
            conflict=conflict,
            conflict_reason=conflict.reason,
            retry_record=retry_record,
        )

    def _rollback(self, transaction, undo_entries, conflict):
        for entry in reversed(undo_entries):
            table = self._tables[entry.table]
            if entry.old_row is None:
                table.pop(entry.primary_key_value, None)
            else:
                table[entry.primary_key_value] = dict(entry.old_row)

        applied_lsns = [entry.statement_lsn for entry in undo_entries]
        if conflict.conflicting_lsn not in applied_lsns:
            applied_lsns.append(conflict.conflicting_lsn)
        applied_tables = []
        for entry in undo_entries:
            if entry.table not in applied_tables:
                applied_tables.append(entry.table)
        if conflict.table not in applied_tables:
            applied_tables.append(conflict.table)

        savepoint = transaction.savepoint
        savepoint_available = bool(savepoint and savepoint.available)
        savepoint_covered_all = bool(
            savepoint
            and savepoint.available
            and savepoint.lsn <= transaction.statements[0].lsn
        )
        if savepoint is None:
            note = "no savepoint supplied; rolled back to the transaction boundary"
        elif not savepoint.available:
            note = "savepoint %s was lost; rolled back to the transaction boundary" % savepoint.name
        elif savepoint_covered_all:
            note = (
                "savepoint %s covers all writes; transaction boundary remains the "
                "cross-table atomic boundary" % savepoint.name
            )
        else:
            note = (
                "savepoint %s starts at LSN %s and does not cover earlier statements; "
                "rolled back all writes to the transaction boundary"
                % (savepoint.name, savepoint.lsn)
            )

        return RollbackRange(
            boundary="TRANSACTION",
            start_lsn=applied_lsns[0],
            end_lsn=applied_lsns[-1],
            statement_lsns=applied_lsns,
            tables=applied_tables,
            savepoint_name=savepoint.name if savepoint else None,
            savepoint_available=savepoint_available,
            savepoint_covered_all=savepoint_covered_all,
            note=note,
        )

    def _commit(self, transaction):
        if transaction.commit_lsn <= self._last_successful_lsn:
            raise ValueError("commit LSN cannot move the replication watermark backwards")
        self._committed_txids.add(transaction.txid)
        self._blocking_transactions.pop(transaction.txid, None)
        self._last_successful_lsn = transaction.commit_lsn
