from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import current_principal
from app.core.clock import to_storage
from app.core.errors import ConflictError
from app.core.security import Principal
from app.database import get_connection, transaction
from app.samples.ledger import INVALIDATION_ON_STATE, ConsumptionLedgerService
from app.samples.ledger_schemas import (
    ConsumptionConfirm,
    ConsumptionCorrection,
    ReservationCreate,
    ReservationRelease,
    SampleQuarantine,
)
from app.samples.repository import SampleRepository
from app.services.audit import AuditService

router = APIRouter(prefix="/api/samples", tags=["实验消耗账本"])


@router.post("/{sample_id}/consumption-reservations", status_code=status.HTTP_201_CREATED)
def reserve_consumption(sample_id: int, payload: ReservationCreate, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ConsumptionLedgerService(connection).reserve(principal, sample_id, payload.model_dump())


@router.get("/consumption-reservations/{reservation_id}")
def get_reservation(reservation_id: int, principal: Principal = Depends(current_principal)):
    principal.require("samples.read")
    return ConsumptionLedgerService(get_connection()).get_reservation(reservation_id)


@router.post("/consumption-reservations/{reservation_id}/confirm", status_code=status.HTTP_201_CREATED)
def confirm_consumption(reservation_id: int, payload: ConsumptionConfirm, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ConsumptionLedgerService(connection).confirm(principal, reservation_id, payload.model_dump())


@router.post("/consumption-reservations/{reservation_id}/release", status_code=status.HTTP_201_CREATED)
def release_reservation(reservation_id: int, payload: ReservationRelease, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ConsumptionLedgerService(connection).release(principal, reservation_id, payload.model_dump())


@router.post("/consumption-reservations/{reservation_id}/corrections", status_code=status.HTTP_201_CREATED)
def correct_consumption(reservation_id: int, payload: ConsumptionCorrection, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ConsumptionLedgerService(connection).correct(principal, reservation_id, payload.model_dump())


@router.get("/consumption-ledger")
def consumption_ledger(
    sample_id: int | None = Query(default=None, gt=0),
    experiment_code: str | None = Query(default=None, min_length=2, max_length=100),
    principal: Principal = Depends(current_principal),
):
    return ConsumptionLedgerService(get_connection()).ledger(
        principal, sample_id=sample_id, experiment_code=experiment_code
    )


@router.get("/experiments/{experiment_code}/consumption-reservations")
def experiment_reservations(experiment_code: str, principal: Principal = Depends(current_principal)):
    service = ConsumptionLedgerService(get_connection())
    return {
        "experiment_code": experiment_code,
        "reservations": service.reservations_for_experiment(principal, experiment_code),
    }


@router.post("/{sample_id}/quarantine", status_code=status.HTTP_201_CREATED)
def quarantine_sample(sample_id: int, payload: SampleQuarantine, principal: Principal = Depends(current_principal)):
    """隔离样品：样品转入 quarantined，未确认消耗预约按规则立即失效并解冻。"""
    principal.require("samples.write")
    with transaction(immediate=True) as connection:
        service = ConsumptionLedgerService(connection)
        samples = SampleRepository(connection)
        sample = samples.get(sample_id)
        if sample["lifecycle_state"] == "quarantined":
            return {"sample": sample, "invalidated_reservations": [], "replayed": True}
        if sample["lifecycle_state"] in {"destroyed", "pending_destruction", "consumed"}:
            raise ConflictError("样品当前状态不能隔离")
        now = to_storage(service.clock.now())
        invalidated = service.invalidate_open_reservations(
            principal, sample_id, INVALIDATION_ON_STATE["quarantined"]
        )
        # 失效处理会推进样品版本号，需重新取最新版本再做状态变更。
        current = samples.get(sample_id)
        updated = samples.set_state(sample_id, "quarantined", current["version"], now)
        samples.append_event(
            sample_id, "sample.quarantined", principal.user_id, now,
            from_state=sample["lifecycle_state"], to_state="quarantined",
            details={"reason": payload.reason, "invalidated_reservation_ids": [item["id"] for item in invalidated]},
        )
        AuditService(connection, service.clock).record(
            principal, "sample.quarantine", "sample", str(sample_id), before=sample, after=updated,
            metadata={"invalidated_reservations": len(invalidated)},
        )
        return {"sample": updated, "invalidated_reservations": invalidated, "replayed": False}
