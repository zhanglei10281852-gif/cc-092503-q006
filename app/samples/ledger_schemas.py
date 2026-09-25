from __future__ import annotations

from pydantic import BaseModel, Field


class ReservationCreate(BaseModel):
    experiment_code: str = Field(min_length=2, max_length=100)
    quantity: float = Field(gt=0)
    idempotency_key: str = Field(min_length=4, max_length=100)
    reservation_code: str | None = Field(default=None, min_length=3, max_length=64)
    expires_at: str | None = Field(default=None, min_length=10, max_length=40)
    note: str = Field(default="", max_length=500)


class ConsumptionConfirm(BaseModel):
    actual_quantity: float = Field(ge=0)
    idempotency_key: str | None = Field(default=None, min_length=4, max_length=100)
    note: str = Field(default="", max_length=500)


class ReservationRelease(BaseModel):
    idempotency_key: str | None = Field(default=None, min_length=4, max_length=100)
    note: str = Field(default="", max_length=500)


class ConsumptionCorrection(BaseModel):
    correct_quantity: float = Field(ge=0)
    reason: str = Field(min_length=2, max_length=500)
    idempotency_key: str | None = Field(default=None, min_length=4, max_length=100)


class SampleQuarantine(BaseModel):
    reason: str = Field(min_length=2, max_length=500)
