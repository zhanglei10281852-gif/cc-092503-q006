"""实验消耗账本：预约（冻结）、确认（结算实际消耗）、释放（取消）、更正（红冲+新分录）。

记账规则：
- reserve：冻结量增加，实际库存不变。
- confirm：按实际消耗扣减库存并解冻预约量；未用部分随结算一并解冻。
- release：实验取消，冻结量全部解冻，库存不变。
- correction：先以反向分录冲销当前生效的消耗（库存回补），再按正确数量记新消耗分录，
  历史分录永不修改、永不删除；冻结量在确认时已结清，更正只调整实际库存。
- invalidate：样品隔离或销毁时，系统按明确规则让未确认预约失效（冻结量释放，库存不变）。

每条分录都带 experiment_code，样品事件与消耗分录共享同一幂等键/关联编号，
研究人员可以从样品事件和实验编号双向对账到每次库存变化。
"""

from __future__ import annotations

import sqlite3
import uuid
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.core.security import Principal
from app.samples.repository import SampleRepository
from app.services.audit import AuditService

# 样品进入这些状态时，未确认预约必须失效（规则：隔离=预约作废但样品保留；
# 待销毁/已销毁=随样品处置一并作废）。
INVALIDATION_ON_STATE = {
    "quarantined": "sample_quarantined",
    "pending_destruction": "sample_pending_destruction",
    "destroyed": "sample_destroyed",
}

_QUANTITY_EPS = 1e-9


class ConsumptionLedgerService:
    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.samples = SampleRepository(connection)
        self.audit = AuditService(connection, self.clock)

    # ------------------------------------------------------------------ 预约

    def reserve(self, principal: Principal, sample_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.consume")
        sample = self.samples.get(sample_id)
        replay = self._find_reservation_replay(sample_id, data["idempotency_key"], data)
        if replay is not None:
            return replay
        self._ensure_bookable(sample)
        quantity = float(data["quantity"])
        if sample["quantity"] - sample["reserved_quantity"] + _QUANTITY_EPS < quantity:
            raise ConflictError("可预约数量不足，并发预约不得透支同一样品")
        now = to_storage(self.clock.now())
        code = data.get("reservation_code") or f"RSV-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO consumption_reservations(
                   reservation_code,sample_id,experiment_code,reserved_quantity,state,
                   operator_user_id,idempotency_key,reserved_at,expires_at,note,created_at,updated_at
               ) VALUES(?,?,?,?,'reserved',?,?,?,?,?,?,?)""",
            (
                code, sample_id, data["experiment_code"], quantity, principal.user_id,
                data["idempotency_key"], now, data.get("expires_at"), data.get("note", ""), now, now,
            ),
        )
        reservation_id = cursor.lastrowid
        # 原子冻结：条件更新保证并发实验不会透支。
        updated = self.connection.execute(
            """UPDATE samples SET reserved_quantity=reserved_quantity+?,version=version+1,updated_at=?
               WHERE id=? AND quantity-reserved_quantity+?>=0""",
            (quantity, now, sample_id, quantity),
        )
        if updated.rowcount != 1:
            raise ConflictError("可预约数量不足，并发预约不得透支同一样品")
        entry = self._insert_entry(
            sample_id, reservation_id, data["experiment_code"], "reserve", quantity,
            operation="reserve",
            on_hand_delta=0.0, frozen_delta=quantity, actor=principal.user_id,
            idempotency_key=data["idempotency_key"], note=data.get("note", ""), now=now,
        )
        self.samples.append_event(
            sample_id, "consumption.reserved", principal.user_id, now,
            quantity_delta=0, from_state=sample["lifecycle_state"],
            details={
                "reservation_id": reservation_id, "reservation_code": code,
                "experiment_code": data["experiment_code"], "reserved_quantity": quantity,
                "ledger_entry_id": entry["id"],
            },
        )
        self.audit.record(principal, "consumption.reserve", "consumption_reservation", str(reservation_id), after=self.get_reservation(reservation_id, principal=principal))
        return self.get_reservation(reservation_id, principal=principal, replayed=False)

    # ------------------------------------------------------------------ 确认

    def confirm(self, principal: Principal, reservation_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("samples.consume")
        reservation = self._get_reservation_row(reservation_id)
        replay = self._find_action_replay(reservation, "confirm", data.get("idempotency_key"))
        if replay is not None:
            return replay
        if reservation["state"] != "reserved":
            raise ConflictError(f"预约当前状态为 {reservation['state']}，不能确认消耗")
        consumed = float(data["actual_quantity"])
        if consumed < 0:
            raise ValidationError("实际消耗不能为负数")
        if consumed > reservation["reserved_quantity"] + _QUANTITY_EPS:
            raise ValidationError("实际消耗不能超过预约数量")
        sample = self.samples.get(reservation["sample_id"])
        if sample["lifecycle_state"] in {"destroyed", "pending_destruction", "quarantined"}:
            raise ConflictError("样品已隔离或进入销毁流程，不能确认消耗")
        now = to_storage(self.clock.now())
        released = round(reservation["reserved_quantity"] - consumed, 9)

        # 先解冻全部预约量，再扣减实际消耗，一次结算完成。
        cursor = self.connection.execute(
            """UPDATE samples SET reserved_quantity=reserved_quantity-?,quantity=quantity-?,
               version=version+1,updated_at=? WHERE id=?
               AND reserved_quantity-?>=0 AND quantity-?>=0""",
            (reservation["reserved_quantity"], consumed, now, sample["id"],
             reservation["reserved_quantity"], consumed),
        )
        if cursor.rowcount != 1:
            raise ConflictError("结算失败：冻结量或库存已变化，请刷新后重试")
        latest = self.samples.get(sample["id"])
        new_state = latest["lifecycle_state"]
        if consumed > _QUANTITY_EPS:
            new_state = "consumed" if latest["quantity"] <= _QUANTITY_EPS else "partially_consumed"
        if new_state != latest["lifecycle_state"]:
            self.samples.set_state(sample["id"], new_state, latest["version"], now)
        self.connection.execute(
            """UPDATE consumption_reservations SET state='confirmed',consumed_quantity=?,
               settled_at=?,version=version+1,updated_at=? WHERE id=? AND state='reserved'""",
            (consumed, now, now, reservation_id),
        )
        entries = []
        # 幂等锚点：有实际消耗挂在 consume 分录，零消耗时挂在未用退回分录上。
        if consumed > _QUANTITY_EPS:
            entries.append(self._insert_entry(
                sample["id"], reservation_id, reservation["experiment_code"], "consume", consumed,
                operation="confirm",
                on_hand_delta=-consumed, frozen_delta=-consumed,
                actor=principal.user_id, idempotency_key=data.get("idempotency_key"),
                note=data.get("note", ""), now=now,
            ))
        if released > _QUANTITY_EPS:
            # 未用部分随确认一并退回可用量（冻结减少、库存不变）。
            entries.append(self._insert_entry(
                sample["id"], reservation_id, reservation["experiment_code"], "release", released,
                operation="confirm",
                on_hand_delta=0.0, frozen_delta=-released, actor=principal.user_id,
                idempotency_key=data.get("idempotency_key") if consumed <= _QUANTITY_EPS else None,
                now=now, note="确认结算时退回未使用数量",
            ))
        self.samples.append_event(
            sample["id"], "consumption.confirmed", principal.user_id, now,
            quantity_delta=-consumed, from_state=sample["lifecycle_state"], to_state=new_state,
            details={
                "reservation_id": reservation_id, "experiment_code": reservation["experiment_code"],
                "reserved_quantity": reservation["reserved_quantity"], "consumed_quantity": consumed,
                "released_quantity": released, "ledger_entry_ids": [item["id"] for item in entries],
            },
        )
        self.audit.record(principal, "consumption.confirm", "consumption_reservation", str(reservation_id),
                         before=reservation, after=self._get_reservation_row(reservation_id))
        result = self.get_reservation(reservation_id, principal=principal)
        result["settled_entries"] = [self._present_entry(item) for item in entries]
        result["replayed"] = False
        return result

    # ------------------------------------------------------------------ 释放

    def release(self, principal: Principal, reservation_id: int, data: dict[str, Any] | None = None) -> dict[str, Any]:
        principal.require("samples.consume")
        data = data or {}
        reservation = self._get_reservation_row(reservation_id)
        replay = self._find_action_replay(reservation, "release", data.get("idempotency_key"))
        if replay is not None:
            return replay
        if reservation["state"] != "reserved":
            raise ConflictError(f"预约当前状态为 {reservation['state']}，不能释放")
        now = to_storage(self.clock.now())
        self.connection.execute(
            """UPDATE samples SET reserved_quantity=reserved_quantity-?,version=version+1,updated_at=?
               WHERE id=? AND reserved_quantity-?>=0""",
            (reservation["reserved_quantity"], now, reservation["sample_id"], reservation["reserved_quantity"]),
        )
        self.connection.execute(
            """UPDATE consumption_reservations SET state='released',settled_at=?,
               version=version+1,updated_at=? WHERE id=? AND state='reserved'""",
            (now, now, reservation_id),
        )
        entry = self._insert_entry(
            reservation["sample_id"], reservation_id, reservation["experiment_code"], "release",
            reservation["reserved_quantity"], operation="release",
            on_hand_delta=0.0,
            frozen_delta=-reservation["reserved_quantity"], actor=principal.user_id,
            idempotency_key=data.get("idempotency_key"), note=data.get("note", "实验取消，释放预约"), now=now,
        )
        self.samples.append_event(
            reservation["sample_id"], "consumption.released", principal.user_id, now,
            quantity_delta=0, details={
                "reservation_id": reservation_id, "experiment_code": reservation["experiment_code"],
                "released_quantity": reservation["reserved_quantity"], "ledger_entry_id": entry["id"],
            },
        )
        self.audit.record(principal, "consumption.release", "consumption_reservation", str(reservation_id),
                         before=reservation, after=self._get_reservation_row(reservation_id))
        return self.get_reservation(reservation_id, principal=principal, replayed=False)

    # ------------------------------------------------------------------ 更正

    def correct(self, principal: Principal, reservation_id: int, data: dict[str, Any]) -> dict[str, Any]:
        """更正已确认消耗：反向分录冲销原消耗，再按正确数量记新分录。

        历史分录保持不变；预约冻结在确认时已结算，更正只影响实际库存，
        库存净变化 = 原消耗 - 正确消耗（多扣则回补，少扣则补扣）。
        """
        principal.require("samples.consume")
        reservation = self._get_reservation_row(reservation_id)
        replay = self._find_action_replay(reservation, "correct", data.get("idempotency_key"))
        if replay is not None:
            return replay
        if reservation["state"] != "confirmed":
            raise ConflictError("只有已确认的预约可以更正")
        corrected = float(data["correct_quantity"])
        if corrected < 0:
            raise ValidationError("更正后的消耗不能为负数")
        sample = self.samples.get(reservation["sample_id"])
        if sample["lifecycle_state"] in {"destroyed", "pending_destruction", "quarantined"}:
            raise ConflictError("样品已隔离或进入销毁流程，不能更正消耗")
        original = float(reservation["consumed_quantity"])
        if abs(corrected - original) <= _QUANTITY_EPS:
            raise ValidationError("更正数量与原消耗一致，无需更正")
        now = to_storage(self.clock.now())
        group = self._next_correction_group()

        reversal_entries: list[dict[str, Any]] = []
        effective_consume_id = self._latest_effective_consume_entry_id(reservation_id)
        if original > _QUANTITY_EPS:
            # 1) 红冲原消耗：库存回补（与原 consume 的库存方向相反），冻结不再变动。
            reversal_entries.append(self._insert_entry(
                sample["id"], reservation_id, reservation["experiment_code"], "consume_reversal",
                original, operation="correct",
                on_hand_delta=original, frozen_delta=0.0,
                actor=principal.user_id, idempotency_key=data.get("idempotency_key"),
                note=data.get("reason", "更正：冲销原确认消耗"), now=now,
                reversal_of=effective_consume_id, correction_group=group,
            ))

        # 2) 按正确数量记新消耗分录。
        new_entries: list[dict[str, Any]] = []
        if corrected > _QUANTITY_EPS:
            new_entries.append(self._insert_entry(
                sample["id"], reservation_id, reservation["experiment_code"], "consume", corrected,
                operation="correct",
                on_hand_delta=-corrected, frozen_delta=0.0,
                actor=principal.user_id,
                idempotency_key=data.get("idempotency_key") if original <= _QUANTITY_EPS else None,
                now=now, note=f"更正后确认消耗（分组 {group}）", correction_group=group,
            ))

        # 3) 一次性把库存调整到正确值：净额 = 原消耗 - 正确消耗。
        net = round(original - corrected, 9)
        if abs(net) > _QUANTITY_EPS:
            cursor = self.connection.execute(
                """UPDATE samples SET quantity=quantity+?,version=version+1,updated_at=?
                   WHERE id=? AND quantity+?>=0 AND quantity+?>=reserved_quantity""",
                (net, now, sample["id"], net, net),
            )
            if cursor.rowcount != 1:
                raise ConflictError("更正后库存不足以容纳当前未确认预约，无法完成更正")
        latest = self.samples.get(sample["id"])
        target_state = "consumed" if latest["quantity"] <= _QUANTITY_EPS else "partially_consumed"
        if latest["lifecycle_state"] != target_state:
            self.samples.set_state(sample["id"], target_state, latest["version"], now)

        self.connection.execute(
            """UPDATE consumption_reservations SET consumed_quantity=?,
               version=version+1,updated_at=? WHERE id=?""",
            (corrected, now, reservation_id),
        )
        all_entries = reversal_entries + new_entries
        self.samples.append_event(
            sample["id"], "consumption.corrected", principal.user_id, now,
            quantity_delta=-net, details={
                "reservation_id": reservation_id, "experiment_code": reservation["experiment_code"],
                "original_quantity": original, "correct_quantity": corrected,
                "net_quantity_delta": -net, "correction_group_id": group,
                "ledger_entry_ids": [item["id"] for item in all_entries],
                "reason": data.get("reason", ""),
            },
        )
        self.audit.record(principal, "consumption.correct", "consumption_reservation", str(reservation_id),
                         before=reservation, after=self._get_reservation_row(reservation_id),
                         metadata={"correction_group_id": group, "original": original, "corrected": corrected})
        result = self.get_reservation(reservation_id, principal=principal)
        result["correction_group_id"] = group
        result["reversal_entries"] = [self._present_entry(item) for item in reversal_entries]
        result["new_entries"] = [self._present_entry(item) for item in new_entries]
        result["replayed"] = False
        return result

    # ------------------------------------------------------- 隔离/销毁联动失效

    def invalidate_open_reservations(self, principal: Principal | None, sample_id: int, reason_code: str) -> list[dict[str, Any]]:
        """样品隔离或销毁时调用：未确认（reserved）预约按规则一律失效，冻结量退回。

        已确认/已释放的预约不受影响。reason_code 必须取自 INVALIDATION_ON_STATE。
        """
        if reason_code not in INVALIDATION_ON_STATE.values():
            raise ValidationError("未知的预约失效规则")
        actor_id = principal.user_id if principal is not None else None
        now = to_storage(self.clock.now())
        rows = self.connection.execute(
            "SELECT * FROM consumption_reservations WHERE sample_id=? AND state='reserved' ORDER BY id",
            (sample_id,),
        ).fetchall()
        invalidated: list[dict[str, Any]] = []
        for row in rows:
            reservation = dict(row)
            self.connection.execute(
                """UPDATE samples SET reserved_quantity=reserved_quantity-?,version=version+1,updated_at=?
                   WHERE id=? AND reserved_quantity-?>=0""",
                (reservation["reserved_quantity"], now, sample_id, reservation["reserved_quantity"]),
            )
            self.connection.execute(
                """UPDATE consumption_reservations SET state='invalidated',settled_at=?,
                   invalidated_reason=?,version=version+1,updated_at=? WHERE id=?""",
                (now, reason_code, now, reservation["id"]),
            )
            entry = self._insert_entry(
                sample_id, reservation["id"], reservation["experiment_code"], "invalidate",
                reservation["reserved_quantity"], operation="invalidate",
                on_hand_delta=0.0,
                frozen_delta=-reservation["reserved_quantity"], actor=actor_id or reservation["operator_user_id"],
                note=f"样品状态变化（{reason_code}），未确认预约失效", now=now,
            )
            self.samples.append_event(
                sample_id, "consumption.invalidated", actor_id, now,
                quantity_delta=0, details={
                    "reservation_id": reservation["id"], "experiment_code": reservation["experiment_code"],
                    "quantity": reservation["reserved_quantity"], "reason": reason_code,
                    "ledger_entry_id": entry["id"],
                },
            )
            if principal is not None:
                self.audit.record(principal, "consumption.invalidate", "consumption_reservation",
                                  str(reservation["id"]), before=reservation,
                                  after=self._get_reservation_row(reservation["id"]),
                                  metadata={"reason": reason_code})
            invalidated.append(self._get_reservation_row(reservation["id"]))
        return invalidated

    # ------------------------------------------------------------------ 查询对账

    def get_reservation(self, reservation_id: int, *, principal: Principal | None = None, replayed: bool = False) -> dict[str, Any]:
        reservation = self._get_reservation_row(reservation_id)
        reservation["replayed"] = replayed
        return reservation

    def ledger(self, principal: Principal, *, sample_id: int | None = None, experiment_code: str | None = None) -> dict[str, Any]:
        principal.require("samples.read")
        clauses: list[str] = []
        params: list[Any] = []
        if sample_id is not None:
            if not self.connection.execute("SELECT 1 FROM samples WHERE id=?", (sample_id,)).fetchone():
                raise NotFoundError("样品不存在")
            clauses.append("e.sample_id=?")
            params.append(sample_id)
        if experiment_code:
            clauses.append("e.experiment_code=?")
            params.append(experiment_code)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self.connection.execute(
            "SELECT * FROM consumption_ledger_entries e" + where + " ORDER BY e.id",
            tuple(params),
        ).fetchall()
        entries = [self._present_entry(row) for row in rows]
        return {
            "filters": {"sample_id": sample_id, "experiment_code": experiment_code},
            "entries": entries,
            "totals": self._totals(entries),
        }

    def reservations_for_experiment(self, principal: Principal, experiment_code: str) -> list[dict[str, Any]]:
        principal.require("samples.read")
        rows = self.connection.execute(
            "SELECT * FROM consumption_reservations WHERE experiment_code=? ORDER BY id",
            (experiment_code,),
        ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------ 内部方法

    def _ensure_bookable(self, sample: dict[str, Any]) -> None:
        if sample["lifecycle_state"] in {"destroyed", "pending_destruction", "quarantined", "consumed"}:
            raise ConflictError("样品当前状态不接受预约")

    def _get_reservation_row(self, reservation_id: int) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM consumption_reservations WHERE id=?", (reservation_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("消耗预约不存在")
        return dict(row)

    def _find_reservation_replay(self, sample_id: int, idempotency_key: str, data: dict[str, Any]) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM consumption_reservations WHERE sample_id=? AND idempotency_key=?",
            (sample_id, idempotency_key),
        ).fetchone()
        if row is None:
            return None
        reservation = dict(row)
        if (
            reservation["experiment_code"] != data["experiment_code"]
            or abs(float(reservation["reserved_quantity"]) - float(data["quantity"])) > _QUANTITY_EPS
        ):
            raise ConflictError("同一幂等键不能用于不同预约请求")
        return self.get_reservation(reservation["id"], replayed=True)

    def _find_action_replay(self, reservation: dict[str, Any], operation: str, idempotency_key: str | None) -> dict[str, Any] | None:
        if not idempotency_key:
            return None
        row = self.connection.execute(
            "SELECT * FROM consumption_ledger_entries WHERE idempotency_key=? AND operation=?",
            (idempotency_key, operation),
        ).fetchone()
        if row is None:
            return None
        existing = dict(row)
        if existing["reservation_id"] != reservation["id"]:
            raise ConflictError("同一幂等键不能用于不同业务操作")
        # 重放返回原业务结果。
        result = self.get_reservation(reservation["id"], replayed=True)
        result["replayed_entry"] = self._present_entry(existing)
        return result

    def _latest_effective_consume_entry_id(self, reservation_id: int) -> int | None:
        # 连续更正时，红冲对象是当前仍生效（未被冲销）的最近一条消耗分录。
        row = self.connection.execute(
            """SELECT e.id FROM consumption_ledger_entries e
               WHERE e.reservation_id=? AND e.entry_type='consume'
               AND NOT EXISTS (
                   SELECT 1 FROM consumption_ledger_entries r WHERE r.reversal_of_id=e.id
               )
               ORDER BY e.id DESC LIMIT 1""",
            (reservation_id,),
        ).fetchone()
        return row["id"] if row else None

    def _next_correction_group(self) -> int:
        return int(self.connection.execute(
            "SELECT COALESCE(MAX(correction_group_id),0)+1 FROM consumption_ledger_entries"
        ).fetchone()[0])

    def _insert_entry(
        self,
        sample_id: int,
        reservation_id: int | None,
        experiment_code: str,
        entry_type: str,
        quantity: float,
        *,
        operation: str,
        on_hand_delta: float,
        frozen_delta: float,
        actor: int,
        idempotency_key: str | None = None,
        note: str = "",
        now: str,
        reversal_of: int | None = None,
        correction_group: int | None = None,
    ) -> dict[str, Any]:
        cursor = self.connection.execute(
            """INSERT INTO consumption_ledger_entries(
                   sample_id,reservation_id,experiment_code,entry_type,operation,quantity,
                   on_hand_delta,frozen_delta,reversal_of_id,correction_group_id,
                   actor_user_id,idempotency_key,note,occurred_at,created_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                sample_id, reservation_id, experiment_code, entry_type, operation, quantity,
                on_hand_delta, frozen_delta, reversal_of, correction_group,
                actor, idempotency_key, note, now, now,
            ),
        )
        return dict(self.connection.execute(
            "SELECT * FROM consumption_ledger_entries WHERE id=?", (cursor.lastrowid,)
        ).fetchone())

    def _present_entry(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        entry = dict(row)
        return entry

    def _totals(self, entries: list[dict[str, Any]]) -> dict[str, float]:
        on_hand = sum(float(entry["on_hand_delta"]) for entry in entries)
        frozen = sum(float(entry["frozen_delta"]) for entry in entries)
        return {"on_hand_delta": round(on_hand, 9), "frozen_delta": round(frozen, 9)}
