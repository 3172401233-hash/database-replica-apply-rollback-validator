from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


INSERT = "INSERT"
UPDATE = "UPDATE"
DELETE = "DELETE"


@dataclass(frozen=True)
class Statement:
    """One logical row change belonging to a source transaction."""

    lsn: int
    table: str
    operation: str
    unique_key: Tuple[str, object]
    row: Optional[Dict[str, object]] = None
    primary_key: Optional[Tuple[str, object]] = None

    def primary_key_value(self):
        if self.primary_key is not None:
            return self.primary_key[1]
        if self.row is None or "id" not in self.row:
            raise ValueError("primary key is required for statement %s" % self.lsn)
        return self.row["id"]


@dataclass(frozen=True)
class Savepoint:
    """A savepoint whose LSN is the first statement it can cover."""

    name: str
    lsn: int
    available: bool = True


@dataclass(frozen=True)
class Transaction:
    txid: str
    start_lsn: int
    commit_lsn: int
    statements: List[Statement]
    savepoint: Optional[Savepoint] = None

    def __post_init__(self):
        if not self.txid:
            raise ValueError("transaction id is required")
        if not self.statements:
            raise ValueError("transaction %s has no statements" % self.txid)
        if self.start_lsn <= 0 or self.commit_lsn <= 0:
            raise ValueError("LSN values must be positive")
        previous_lsn = 0
        for statement in self.statements:
            if statement.lsn <= previous_lsn:
                raise ValueError("statement LSN values must be strictly ordered")
            previous_lsn = statement.lsn
        first_lsn = self.statements[0].lsn
        last_lsn = self.statements[-1].lsn
        if self.start_lsn > first_lsn:
            raise ValueError("start LSN cannot be later than the first statement")
        if self.commit_lsn <= last_lsn:
            raise ValueError("commit LSN must be later than the last statement")
        if self.savepoint is not None and self.savepoint.lsn <= 0:
            raise ValueError("savepoint LSN must be positive")


@dataclass(frozen=True)
class UniqueKeyConflict:
    table: str
    key_name: str
    key_value: object
    conflicting_lsn: int
    existing_primary_key: object
    reason: str


@dataclass(frozen=True)
class RollbackRange:
    boundary: str
    start_lsn: int
    end_lsn: int
    statement_lsns: List[int]
    tables: List[str]
    savepoint_name: Optional[str]
    savepoint_available: bool
    savepoint_covered_all: bool
    note: str


@dataclass(frozen=True)
class PartialApplication:
    """Prefix statements already visible on the target after a crash or retry."""

    txid: str
    applied_statement_lsns: List[int]
    tables: List[str]

    def __post_init__(self):
        if not self.txid:
            raise ValueError("transaction id is required")
        if list(self.applied_statement_lsns) != sorted(self.applied_statement_lsns):
            raise ValueError("partial application LSN values must be ordered")
        if len(set(self.applied_statement_lsns)) != len(self.applied_statement_lsns):
            raise ValueError("partial application contains duplicate LSN values")


@dataclass(frozen=True)
class RetryRecord:
    txid: str
    attempt: int
    last_lsn: int
    blocking_lsn: int
    result: str
    reason: str


@dataclass(frozen=True)
class ApplyResult:
    status: str
    txid: str
    committed: bool
    visible_tables: Dict[str, Dict[object, Dict[str, object]]]
    next_apply_lsn: int
    last_successful_lsn: int
    partial_application: Optional[PartialApplication]
    rollback: Optional[RollbackRange]
    conflict: Optional[UniqueKeyConflict]
    conflict_reason: Optional[str]
    retry_record: Optional[RetryRecord]


@dataclass
class UndoEntry:
    statement_lsn: int
    table: str
    primary_key_value: object
    old_row: Optional[Dict[str, object]]


@dataclass
class _Conflict(Exception):
    conflict: UniqueKeyConflict


def copied_tables(tables):
    return {
        table: {key: dict(row) for key, row in rows.items()}
        for table, rows in tables.items()
    }


def copied_retry_records(records):
    return [RetryRecord(**record.__dict__) for record in records]
