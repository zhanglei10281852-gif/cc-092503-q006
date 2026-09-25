from __future__ import annotations

import sqlite3
import uuid
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.core.security import Principal
from app.samples.repository import SampleRepository
from app.services.audit import AuditService

# 未确认预约在样品隔离或销毁时失效的原因，写入预约与账本分录便于对账
EXPIRY_SAMPLE_QUARANTINED = "sample_quarantined"
EXPIRY_SAMPLE_DESTROYED = "sample_destroyed"

# 禁止发起新消耗预约的生命周期状态（与直接消耗接口保持一致）
RESERVE_FORBIDDEN_STATES = {"destroyed", "pending_destruction", "quarantined"}

# 允许更正的分录类型：确认消耗及其更正确认分录（形成更正链，历史分录永不覆盖）
CORRECTABLE_ENTRY_TYPES = {"confirm", "correction_reentry"}


def _row(row: sqlite3.Row | None, message: str) -> dict[str, Any]:
    if row is None:
        raise NotFoundError(message)
    return dict(row)


class ConsumptionLedgerRepository:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def create_reservation(
        self,
        reservation_code: str,
        sample_id: int,
        experiment_code: str,
        quantity: float,
        idempotency_key: str,
        operator_user_id: int,
        note: str,
        now: str,
    ) -> dict[str, Any]:
        cursor = self.connection.execute(
            """INSERT INTO consumption_reservations(
                   reservation_code,sample_id,experiment_code,quantity,state,
                   idempotency_key,operator_user_id,note,created_at,updated_at
               ) VALUES(?,?,?,?,'active',?,?,?,?,?)""",
            (reservation_code, sample_id, experiment_code, quantity, idempotency_key, operator_user_id, note, now, now),
        )
        return self.get_reservation(cursor.lastrowid)

    def get_reservation(self, reservation_id: int) -> dict[str, Any]:
        return _row(
            self.connection.execute(
                "SELECT * FROM consumption_reservations WHERE id=?", (reservation_id,)
            ).fetchone(),
            "消耗预约不存在",
        )

    def reservation_by_key(self, sample_id: int, idempotency_key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM consumption_reservations WHERE sample_id=? AND idempotency_key=?",
            (sample_id, idempotency_key),
        ).fetchone()
        return dict(row) if row else None

    def transition_reservation(
        self,
        reservation_id: int,
        expected_state: str,
        new_state: str,
        now: str,
        *,
        confirmed_quantity: float | None = None,
        confirm_key: str | None = None,
        release_key: str | None = None,
        expiry_reason: str | None = None,
    ) -> dict[str, Any]:
        cursor = self.connection.execute(
            """UPDATE consumption_reservations
               SET state=?,confirmed_quantity=COALESCE(?,confirmed_quantity),
                   confirm_idempotency_key=COALESCE(?,confirm_idempotency_key),
                   release_idempotency_key=COALESCE(?,release_idempotency_key),
                   expiry_reason=COALESCE(?,expiry_reason),
                   settled_at=?,version=version+1,updated_at=?
               WHERE id=? AND state=?""",
            (new_state, confirmed_quantity, confirm_key, release_key, expiry_reason, now, now, reservation_id, expected_state),
        )
        if cursor.rowcount != 1:
            raise ConflictError("预约状态已变化，请刷新后重试")
        return self.get_reservation(reservation_id)

    def active_for_sample(self, sample_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM consumption_reservations WHERE sample_id=? AND state='active' ORDER BY id",
            (sample_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_for_sample(self, sample_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM consumption_reservations WHERE sample_id=? ORDER BY id DESC",
            (sample_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def active_reserved_sum(self, sample_id: int) -> float:
        value = self.connection.execute(
            "SELECT COALESCE(SUM(quantity),0) FROM consumption_reservations WHERE sample_id=? AND state='active'",
            (sample_id,),
        ).fetchone()[0]
        return float(value)

    def append_entry(
        self,
        *,
        reservation_id: int | None,
        sample_id: int,
        experiment_code: str,
        entry_type: str,
        quantity_delta: float,
        reserved_delta: float,
        operator_user_id: int,
        now: str,
        reverses_entry_id: int | None = None,
        idempotency_key: str | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        entry_code = f"CLE-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO consumption_ledger_entries(
                   entry_code,reservation_id,sample_id,experiment_code,entry_type,
                   quantity_delta,reserved_delta,reverses_entry_id,idempotency_key,
                   operator_user_id,note,occurred_at,created_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                entry_code, reservation_id, sample_id, experiment_code, entry_type,
                quantity_delta, reserved_delta, reverses_entry_id, idempotency_key,
                operator_user_id, note, now, now,
            ),
        )
        return self.get_entry(cursor.lastrowid)

    def get_entry(self, entry_id: int) -> dict[str, Any]:
        return _row(
            self.connection.execute(
                "SELECT * FROM consumption_ledger_entries WHERE id=?", (entry_id,)
            ).fetchone(),
            "账本分录不存在",
        )

    def entries_for_reservation(self, reservation_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM consumption_ledger_entries WHERE reservation_id=? ORDER BY id",
            (reservation_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def entries_for_sample(self, sample_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """SELECT e.*,r.reservation_code
               FROM consumption_ledger_entries e
               LEFT JOIN consumption_reservations r ON r.id=e.reservation_id
               WHERE e.sample_id=? ORDER BY e.id""",
            (sample_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def entries_for_experiment(self, experiment_code: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """SELECT e.*,s.sample_code,r.reservation_code
               FROM consumption_ledger_entries e
               JOIN samples s ON s.id=e.sample_id
               LEFT JOIN consumption_reservations r ON r.id=e.reservation_id
               WHERE e.experiment_code=? ORDER BY e.id""",
            (experiment_code,),
        ).fetchall()
        return [dict(row) for row in rows]

    def reversal_of(self, entry_id: int) -> dict[str, Any] | None:
        row = self.connection.execute(
            """SELECT * FROM consumption_ledger_entries
               WHERE reverses_entry_id=? AND entry_type='correction_reversal'""",
            (entry_id,),
        ).fetchone()
        return dict(row) if row else None

    def reentry_of(self, entry_id: int) -> dict[str, Any] | None:
        row = self.connection.execute(
            """SELECT * FROM consumption_ledger_entries
               WHERE reverses_entry_id=? AND entry_type='correction_reentry'""",
            (entry_id,),
        ).fetchone()
        return dict(row) if row else None

    def reversal_by_key(self, sample_id: int, idempotency_key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            """SELECT * FROM consumption_ledger_entries
               WHERE sample_id=? AND idempotency_key=? AND entry_type='correction_reversal'""",
            (sample_id, idempotency_key),
        ).fetchone()
        return dict(row) if row else None


class ConsumptionLedgerService:
    """消耗账本：预约冻结、确认结算、释放退回与反向更正。

    库存语义：samples.quantity 为实际库存，samples.reserved_quantity 为冻结量
    （借用与消耗预约共用），可用量 = quantity - reserved_quantity，任何操作不得透支。
    """

    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.repository = ConsumptionLedgerRepository(connection)
        self.samples = SampleRepository(connection)
        self.audit = AuditService(connection, self.clock)

    def reserve(self, principal: Principal, sample_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.consume")
        sample = self.samples.get(sample_id)
        if sample["lifecycle_state"] in RESERVE_FORBIDDEN_STATES:
            raise ConflictError("当前状态禁止预约消耗")
        existing = self.repository.reservation_by_key(sample_id, data["idempotency_key"])
        if existing:
            if existing["experiment_code"] != data["experiment_code"] or abs(existing["quantity"] - data["quantity"]) > 1e-9:
                raise ConflictError("同一幂等键不能用于不同请求")
            return {"reservation": existing, "sample": self.samples.get(sample_id), "replayed": True}
        now = to_storage(self.clock.now())
        cursor = self.connection.execute(
            """UPDATE samples SET reserved_quantity=reserved_quantity+?,version=version+1,updated_at=?
               WHERE id=? AND quantity-reserved_quantity-?>=0""",
            (data["quantity"], now, sample_id, data["quantity"]),
        )
        if cursor.rowcount != 1:
            raise ConflictError("可用数量不足，无法冻结预约数量")
        reservation = self.repository.create_reservation(
            f"RSV-{uuid.uuid4().hex[:12]}",
            sample_id,
            data["experiment_code"],
            data["quantity"],
            data["idempotency_key"],
            principal.user_id,
            data.get("note", ""),
            now,
        )
        entry = self.repository.append_entry(
            reservation_id=reservation["id"],
            sample_id=sample_id,
            experiment_code=data["experiment_code"],
            entry_type="reserve",
            quantity_delta=0,
            reserved_delta=data["quantity"],
            operator_user_id=principal.user_id,
            idempotency_key=data["idempotency_key"],
            note=data.get("note", ""),
            now=now,
        )
        self.samples.append_event(
            sample_id,
            "reservation.created",
            principal.user_id,
            now,
            details={
                "reservation_code": reservation["reservation_code"],
                "experiment_code": data["experiment_code"],
                "quantity": data["quantity"],
                "entry_code": entry["entry_code"],
            },
        )
        self.audit.record(
            principal,
            "consumption.reserve",
            "consumption_reservation",
            str(reservation["id"]),
            after=reservation,
            metadata={"experiment_code": data["experiment_code"], "quantity": data["quantity"]},
        )
        return {"reservation": reservation, "sample": self.samples.get(sample_id), "replayed": False}

    def confirm(self, principal: Principal, reservation_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.consume")
        reservation = self.repository.get_reservation(reservation_id)
        if reservation["state"] == "confirmed":
            same_key = reservation["confirm_idempotency_key"] == data["idempotency_key"]
            same_quantity = abs(float(reservation["confirmed_quantity"]) - data["actual_quantity"]) <= 1e-9
            if same_key and same_quantity:
                entries = [
                    item
                    for item in self.repository.entries_for_reservation(reservation_id)
                    if item["entry_type"] == "confirm"
                ]
                return {
                    "reservation": reservation,
                    "entries": entries,
                    "sample": self.samples.get(reservation["sample_id"]),
                    "replayed": True,
                }
            raise ConflictError("预约已确认，不能重复确认")
        if reservation["state"] == "released":
            raise ConflictError("预约已释放，不能确认消耗")
        if reservation["state"] == "expired":
            raise ConflictError("预约已失效，不能确认消耗")
        actual = data["actual_quantity"]
        if actual - reservation["quantity"] > 1e-9:
            raise ConflictError("确认数量不能超过预约数量")
        sample = self.samples.get(reservation["sample_id"])
        now = to_storage(self.clock.now())
        new_quantity = sample["quantity"] - actual
        new_reserved = sample["reserved_quantity"] - reservation["quantity"]
        if new_quantity == 0 and new_reserved == 0:
            new_state = "consumed"
        elif sample["lifecycle_state"] == "loaned":
            new_state = "loaned"
        elif actual > 0:
            new_state = "partially_consumed"
        else:
            new_state = sample["lifecycle_state"]
        cursor = self.connection.execute(
            """UPDATE samples SET quantity=quantity-?,reserved_quantity=reserved_quantity-?,
               lifecycle_state=?,version=version+1,updated_at=?
               WHERE id=? AND quantity-?>=0 AND reserved_quantity-?>=0""",
            (actual, reservation["quantity"], new_state, now, sample["id"], actual, reservation["quantity"]),
        )
        if cursor.rowcount != 1:
            raise ConflictError("样品库存已变化，无法完成确认")
        updated = self.repository.transition_reservation(
            reservation_id,
            "active",
            "confirmed",
            now,
            confirmed_quantity=actual,
            confirm_key=data["idempotency_key"],
        )
        entry = self.repository.append_entry(
            reservation_id=reservation_id,
            sample_id=sample["id"],
            experiment_code=reservation["experiment_code"],
            entry_type="confirm",
            quantity_delta=-actual,
            reserved_delta=-reservation["quantity"],
            operator_user_id=principal.user_id,
            idempotency_key=data["idempotency_key"],
            note=data.get("note", ""),
            now=now,
        )
        self.samples.append_event(
            sample["id"],
            "reservation.confirmed",
            principal.user_id,
            now,
            quantity_delta=-actual,
            from_state=sample["lifecycle_state"],
            to_state=new_state,
            details={
                "reservation_code": reservation["reservation_code"],
                "experiment_code": reservation["experiment_code"],
                "confirmed_quantity": actual,
                "released_quantity": round(reservation["quantity"] - actual, 9),
                "entry_code": entry["entry_code"],
            },
        )
        self.audit.record(
            principal,
            "consumption.confirm",
            "consumption_reservation",
            str(reservation_id),
            before=reservation,
            after=updated,
            metadata={"experiment_code": reservation["experiment_code"], "confirmed_quantity": actual},
        )
        return {
            "reservation": updated,
            "entries": [entry],
            "sample": self.samples.get(sample["id"]),
            "replayed": False,
        }

    def release(self, principal: Principal, reservation_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.consume")
        reservation = self.repository.get_reservation(reservation_id)
        if reservation["state"] == "released":
            if reservation["release_idempotency_key"] == data["idempotency_key"]:
                entries = [
                    item
                    for item in self.repository.entries_for_reservation(reservation_id)
                    if item["entry_type"] == "release"
                ]
                return {
                    "reservation": reservation,
                    "entries": entries,
                    "sample": self.samples.get(reservation["sample_id"]),
                    "replayed": True,
                }
            raise ConflictError("预约已释放，不能重复释放")
        if reservation["state"] == "confirmed":
            raise ConflictError("预约已确认，不能释放")
        if reservation["state"] == "expired":
            raise ConflictError("预约已失效，不能释放")
        sample = self.samples.get(reservation["sample_id"])
        now = to_storage(self.clock.now())
        cursor = self.connection.execute(
            """UPDATE samples SET reserved_quantity=reserved_quantity-?,version=version+1,updated_at=?
               WHERE id=? AND reserved_quantity-?>=0""",
            (reservation["quantity"], now, sample["id"], reservation["quantity"]),
        )
        if cursor.rowcount != 1:
            raise ConflictError("样品冻结量已变化，无法释放预约")
        updated = self.repository.transition_reservation(
            reservation_id,
            "active",
            "released",
            now,
            release_key=data["idempotency_key"],
        )
        entry = self.repository.append_entry(
            reservation_id=reservation_id,
            sample_id=sample["id"],
            experiment_code=reservation["experiment_code"],
            entry_type="release",
            quantity_delta=0,
            reserved_delta=-reservation["quantity"],
            operator_user_id=principal.user_id,
            idempotency_key=data["idempotency_key"],
            note=data.get("reason", ""),
            now=now,
        )
        self.samples.append_event(
            sample["id"],
            "reservation.released",
            principal.user_id,
            now,
            details={
                "reservation_code": reservation["reservation_code"],
                "experiment_code": reservation["experiment_code"],
                "quantity": reservation["quantity"],
                "reason": data.get("reason", ""),
                "entry_code": entry["entry_code"],
            },
        )
        self.audit.record(
            principal,
            "consumption.release",
            "consumption_reservation",
            str(reservation_id),
            before=reservation,
            after=updated,
            metadata={"experiment_code": reservation["experiment_code"]},
        )
        return {
            "reservation": updated,
            "entries": [entry],
            "sample": self.samples.get(sample["id"]),
            "replayed": False,
        }

    def correct(self, principal: Principal, entry_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.consume")
        entry = self.repository.get_entry(entry_id)
        if entry["entry_type"] not in CORRECTABLE_ENTRY_TYPES:
            raise ValidationError("只有确认消耗类分录可以更正")
        same_key_reversal = self.repository.reversal_by_key(entry["sample_id"], data["idempotency_key"])
        if same_key_reversal:
            reentry = self.repository.reentry_of(same_key_reversal["reverses_entry_id"])
            same_entry = same_key_reversal["reverses_entry_id"] == entry_id
            same_quantity = reentry is not None and abs(reentry["quantity_delta"] + data["corrected_quantity"]) <= 1e-9
            if same_entry and same_quantity:
                return {
                    "reversal": same_key_reversal,
                    "reentry": reentry,
                    "sample": self.samples.get(entry["sample_id"]),
                    "replayed": True,
                }
            raise ConflictError("同一幂等键不能用于不同请求")
        if self.repository.reversal_of(entry_id):
            raise ConflictError("该分录已被更正，不能重复更正")
        old_actual = -entry["quantity_delta"]
        corrected = data["corrected_quantity"]
        if abs(corrected - old_actual) <= 1e-9:
            raise ValidationError("更正数量与原确认数量相同")
        sample = self.samples.get(entry["sample_id"])
        if sample["lifecycle_state"] in {"destroyed", "pending_destruction"}:
            raise ConflictError("样品已销毁，无法更正消耗分录")
        delta = round(old_actual - corrected, 9)
        now = to_storage(self.clock.now())
        new_quantity = sample["quantity"] + delta
        if new_quantity == 0 and sample["reserved_quantity"] == 0:
            new_state = "consumed"
        elif sample["lifecycle_state"] == "consumed":
            new_state = "partially_consumed"
        elif sample["lifecycle_state"] == "available" and delta < 0:
            new_state = "partially_consumed"
        else:
            new_state = sample["lifecycle_state"]
        cursor = self.connection.execute(
            """UPDATE samples SET quantity=quantity+?,lifecycle_state=?,version=version+1,updated_at=?
               WHERE id=? AND quantity+?>=0 AND reserved_quantity<=quantity+?""",
            (delta, new_state, now, sample["id"], delta, delta),
        )
        if cursor.rowcount != 1:
            raise ConflictError("可用数量不足，无法按更正数量调整库存")
        reversal = self.repository.append_entry(
            reservation_id=entry["reservation_id"],
            sample_id=sample["id"],
            experiment_code=entry["experiment_code"],
            entry_type="correction_reversal",
            quantity_delta=-entry["quantity_delta"],
            reserved_delta=-entry["reserved_delta"],
            reverses_entry_id=entry_id,
            operator_user_id=principal.user_id,
            idempotency_key=data["idempotency_key"],
            note=data["reason"],
            now=now,
        )
        reentry = self.repository.append_entry(
            reservation_id=entry["reservation_id"],
            sample_id=sample["id"],
            experiment_code=entry["experiment_code"],
            entry_type="correction_reentry",
            quantity_delta=-corrected,
            reserved_delta=entry["reserved_delta"],
            reverses_entry_id=entry_id,
            operator_user_id=principal.user_id,
            idempotency_key=data["idempotency_key"],
            note=data["reason"],
            now=now,
        )
        self.samples.append_event(
            sample["id"],
            "consumption.corrected",
            principal.user_id,
            now,
            quantity_delta=delta,
            from_state=sample["lifecycle_state"],
            to_state=new_state,
            details={
                "experiment_code": entry["experiment_code"],
                "corrected_entry_code": entry["entry_code"],
                "reversal_entry_code": reversal["entry_code"],
                "reentry_entry_code": reentry["entry_code"],
                "previous_quantity": old_actual,
                "corrected_quantity": corrected,
                "reason": data["reason"],
            },
        )
        self.audit.record(
            principal,
            "consumption.correct",
            "consumption_ledger_entry",
            str(entry_id),
            before=entry,
            after=reentry,
            metadata={"experiment_code": entry["experiment_code"], "corrected_quantity": corrected},
        )
        return {
            "reversal": reversal,
            "reentry": reentry,
            "sample": self.samples.get(sample["id"]),
            "replayed": False,
        }

    def expire_active_for_sample(
        self,
        principal: Principal,
        sample_id: int,
        reason: str,
        now: str | None = None,
    ) -> list[dict[str, Any]]:
        """使样品全部未确认预约失效并释放冻结量（隔离/销毁的统一规则）。"""
        active = self.repository.active_for_sample(sample_id)
        if not active:
            return []
        now = now or to_storage(self.clock.now())
        total = round(sum(item["quantity"] for item in active), 9)
        cursor = self.connection.execute(
            """UPDATE samples SET reserved_quantity=MAX(0,ROUND(reserved_quantity-?,9)),version=version+1,updated_at=?
               WHERE id=? AND reserved_quantity+1e-9>=?""",
            (total, now, sample_id, total),
        )
        if cursor.rowcount != 1:
            raise ConflictError("样品冻结量已变化，无法失效预约")
        expired = []
        for reservation in active:
            updated = self.repository.transition_reservation(
                reservation["id"], "active", "expired", now, expiry_reason=reason
            )
            entry = self.repository.append_entry(
                reservation_id=reservation["id"],
                sample_id=sample_id,
                experiment_code=reservation["experiment_code"],
                entry_type="expire",
                quantity_delta=0,
                reserved_delta=-reservation["quantity"],
                operator_user_id=principal.user_id,
                note=reason,
                now=now,
            )
            self.samples.append_event(
                sample_id,
                "reservation.expired",
                principal.user_id,
                now,
                details={
                    "reservation_code": reservation["reservation_code"],
                    "experiment_code": reservation["experiment_code"],
                    "quantity": reservation["quantity"],
                    "reason": reason,
                    "entry_code": entry["entry_code"],
                },
            )
            expired.append(updated)
        self.audit.record(
            principal,
            "consumption.expire",
            "sample",
            str(sample_id),
            metadata={
                "reason": reason,
                "expired_count": len(expired),
                "reservation_codes": [item["reservation_code"] for item in expired],
            },
        )
        return expired

    def reservation_detail(self, principal: Principal, reservation_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        reservation = self.repository.get_reservation(reservation_id)
        return {
            "reservation": reservation,
            "entries": self.repository.entries_for_reservation(reservation_id),
        }

    def list_reservations(self, principal: Principal, sample_id: int) -> list[dict[str, Any]]:
        principal.require("samples.read")
        self.samples.get(sample_id)
        return self.repository.list_for_sample(sample_id)

    def sample_ledger(self, principal: Principal, sample_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        sample = self.samples.get(sample_id)
        if sample.get("location_sensitivity") != "normal" and not (
            "*" in principal.permissions or "locations.read_sensitive" in principal.permissions
        ):
            sample["location_code"] = f"MASKED-{sample['location_id']:04d}" if sample.get("location_id") else None
        entries = self.repository.entries_for_sample(sample_id)
        quantity_sum = round(sum(item["quantity_delta"] for item in entries), 9)
        reserved_sum = round(sum(item["reserved_delta"] for item in entries), 9)
        active_reserved = round(self.repository.active_reserved_sum(sample_id), 9)
        return {
            "sample": sample,
            "entries": entries,
            "totals": {
                "entry_count": len(entries),
                "confirmed_consumption": round(-quantity_sum, 9),
                "reserved_delta_sum": reserved_sum,
                "active_reserved": active_reserved,
                "consistent": abs(reserved_sum - active_reserved) <= 1e-9,
            },
        }

    def experiment_ledger(self, principal: Principal, experiment_code: str) -> dict[str, Any]:
        principal.require("samples.read")
        entries = self.repository.entries_for_experiment(experiment_code)
        quantity_sum = round(sum(item["quantity_delta"] for item in entries), 9)
        reserved_sum = round(sum(item["reserved_delta"] for item in entries), 9)
        return {
            "experiment_code": experiment_code,
            "entries": entries,
            "totals": {
                "entry_count": len(entries),
                "confirmed_consumption": round(-quantity_sum, 9),
                "reserved_delta_sum": reserved_sum,
                "sample_codes": sorted({item["sample_code"] for item in entries}),
            },
        }
