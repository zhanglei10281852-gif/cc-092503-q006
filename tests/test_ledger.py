from __future__ import annotations

from threading import Thread

from app.core.errors import ConflictError
from app.core.security import Principal


def create_sample(client, admin, quantity=100, sample_code="LED-001"):
    location = client.post(
        "/api/samples/locations",
        headers=admin["headers"],
        json={"code": f"LOC-{sample_code}", "building": "科研楼", "room": "常温间", "cabinet": "柜二", "shelf": "一层", "sensitivity": "normal", "capacity_units": 100},
    )
    assert location.status_code == 201, location.text
    batch = client.post(
        "/api/samples/batches",
        headers=admin["headers"],
        json={"batch_code": f"BATCH-{sample_code}", "project_code": "P-LEDGER", "expected_count": 1},
    )
    assert batch.status_code == 201, batch.text
    sample = client.post(
        "/api/samples",
        headers=admin["headers"],
        json={"sample_code": sample_code, "batch_id": batch.json()["id"], "sample_type": "土壤", "quantity": quantity, "unit": "g", "location_id": location.json()["id"]},
    )
    assert sample.status_code == 201, sample.text
    return sample.json()


def reserve(client, admin, sample_id, quantity, key, experiment="EXP-LEDGER"):
    return client.post(
        f"/api/samples/{sample_id}/reservations",
        headers=admin["headers"],
        json={"experiment_code": experiment, "quantity": quantity, "idempotency_key": key, "note": "上机预约"},
    )


def test_reserve_freezes_available_quantity(client, admin):
    sample = create_sample(client, admin)
    response = reserve(client, admin, sample["id"], 30, "rsv-0001")
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["replayed"] is False
    assert body["reservation"]["state"] == "active"
    assert body["sample"]["quantity"] == 100
    assert body["sample"]["reserved_quantity"] == 30
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    event = detail["events"][-1]
    assert event["event_type"] == "reservation.created"
    assert event["details"]["experiment_code"] == "EXP-LEDGER"


def test_reserve_replay_and_key_conflict(client, admin):
    sample = create_sample(client, admin)
    first = reserve(client, admin, sample["id"], 30, "rsv-0002")
    second = reserve(client, admin, sample["id"], 30, "rsv-0002")
    assert first.status_code == second.status_code == 201
    assert second.json()["replayed"] is True
    assert second.json()["reservation"]["id"] == first.json()["reservation"]["id"]
    assert second.json()["sample"]["reserved_quantity"] == 30
    different = client.post(
        f"/api/samples/{sample['id']}/reservations",
        headers=admin["headers"],
        json={"experiment_code": "EXP-OTHER", "quantity": 5, "idempotency_key": "rsv-0002"},
    )
    assert different.status_code == 409


def test_reserve_cannot_overdraw_available(client, admin):
    sample = create_sample(client, admin, quantity=10)
    assert reserve(client, admin, sample["id"], 6, "rsv-0003").status_code == 201
    overdraw = reserve(client, admin, sample["id"], 6, "rsv-0004")
    assert overdraw.status_code == 409
    assert reserve(client, admin, sample["id"], 4, "rsv-0005").status_code == 201


def test_concurrent_reservations_never_overdraw(client, admin):
    sample = create_sample(client, admin, quantity=10)
    from app.database import transaction
    from app.samples.ledger import ConsumptionLedgerService

    principal = Principal(
        user_id=admin["body"]["user"]["id"],
        username="admin",
        display_name="样品平台主管",
        department_id=None,
        permissions=frozenset({"*"}),
        session_id=1,
    )
    outcomes = []

    def worker(index: int) -> None:
        try:
            with transaction(immediate=True) as connection:
                ConsumptionLedgerService(connection).reserve(
                    principal,
                    sample["id"],
                    {"experiment_code": f"EXP-C{index}", "quantity": 4, "idempotency_key": f"conc-{index}", "note": ""},
                )
            outcomes.append("reserved")
        except ConflictError:
            outcomes.append("rejected")

    threads = [Thread(target=worker, args=(index,)) for index in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert outcomes.count("reserved") == 2
    assert outcomes.count("rejected") == 2
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["reserved_quantity"] == 8
    assert detail["quantity"] == 10


def test_confirm_settles_actual_and_unused_in_one_step(client, admin):
    sample = create_sample(client, admin)
    reservation = reserve(client, admin, sample["id"], 30, "rsv-0010").json()["reservation"]
    payload = {"actual_quantity": 12, "idempotency_key": "cfm-0010", "note": "实际用量"}
    confirmed = client.post(
        f"/api/samples/reservations/{reservation['id']}/confirm", headers=admin["headers"], json=payload
    )
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body["reservation"]["state"] == "confirmed"
    assert body["reservation"]["confirmed_quantity"] == 12
    # 实际消耗 12，未用 18 随冻结量一次结算退回
    assert body["sample"]["quantity"] == 88
    assert body["sample"]["reserved_quantity"] == 0
    assert body["sample"]["lifecycle_state"] == "partially_consumed"
    entry = body["entries"][0]
    assert entry["entry_type"] == "confirm"
    assert entry["quantity_delta"] == -12
    assert entry["reserved_delta"] == -30
    replay = client.post(
        f"/api/samples/reservations/{reservation['id']}/confirm", headers=admin["headers"], json=payload
    )
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    assert replay.json()["entries"][0]["id"] == entry["id"]
    assert replay.json()["sample"]["quantity"] == 88
    conflict = client.post(
        f"/api/samples/reservations/{reservation['id']}/confirm",
        headers=admin["headers"],
        json={"actual_quantity": 5, "idempotency_key": "cfm-other"},
    )
    assert conflict.status_code == 409


def test_confirm_cannot_exceed_reserved_quantity(client, admin):
    sample = create_sample(client, admin)
    reservation = reserve(client, admin, sample["id"], 20, "rsv-0011").json()["reservation"]
    response = client.post(
        f"/api/samples/reservations/{reservation['id']}/confirm",
        headers=admin["headers"],
        json={"actual_quantity": 21, "idempotency_key": "cfm-0011"},
    )
    assert response.status_code == 409


def test_release_returns_full_reserved_amount(client, admin):
    sample = create_sample(client, admin)
    reservation = reserve(client, admin, sample["id"], 25, "rsv-0012").json()["reservation"]
    payload = {"idempotency_key": "rel-0012", "reason": "实验取消"}
    released = client.post(
        f"/api/samples/reservations/{reservation['id']}/release", headers=admin["headers"], json=payload
    )
    assert released.status_code == 200, released.text
    body = released.json()
    assert body["reservation"]["state"] == "released"
    assert body["sample"]["quantity"] == 100
    assert body["sample"]["reserved_quantity"] == 0
    assert body["entries"][0]["entry_type"] == "release"
    replay = client.post(
        f"/api/samples/reservations/{reservation['id']}/release", headers=admin["headers"], json=payload
    )
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    confirm_after_release = client.post(
        f"/api/samples/reservations/{reservation['id']}/confirm",
        headers=admin["headers"],
        json={"actual_quantity": 1, "idempotency_key": "cfm-0012"},
    )
    assert confirm_after_release.status_code == 409


def test_correction_uses_reversal_and_reentry_without_rewriting_history(client, admin):
    sample = create_sample(client, admin)
    reservation = reserve(client, admin, sample["id"], 30, "rsv-0013").json()["reservation"]
    confirmed = client.post(
        f"/api/samples/reservations/{reservation['id']}/confirm",
        headers=admin["headers"],
        json={"actual_quantity": 12, "idempotency_key": "cfm-0013"},
    ).json()
    confirm_entry = confirmed["entries"][0]
    payload = {"corrected_quantity": 10, "idempotency_key": "cor-0013", "reason": "录错数量"}
    corrected = client.post(
        f"/api/samples/ledger-entries/{confirm_entry['id']}/corrections", headers=admin["headers"], json=payload
    )
    assert corrected.status_code == 201, corrected.text
    body = corrected.json()
    assert body["reversal"]["entry_type"] == "correction_reversal"
    assert body["reversal"]["quantity_delta"] == 12
    assert body["reversal"]["reverses_entry_id"] == confirm_entry["id"]
    assert body["reentry"]["entry_type"] == "correction_reentry"
    assert body["reentry"]["quantity_delta"] == -10
    assert body["sample"]["quantity"] == 90
    replay = client.post(
        f"/api/samples/ledger-entries/{confirm_entry['id']}/corrections", headers=admin["headers"], json=payload
    )
    assert replay.status_code == 201
    assert replay.json()["replayed"] is True
    assert replay.json()["reversal"]["id"] == body["reversal"]["id"]
    duplicate = client.post(
        f"/api/samples/ledger-entries/{confirm_entry['id']}/corrections",
        headers=admin["headers"],
        json={"corrected_quantity": 8, "idempotency_key": "cor-other", "reason": "再次更正"},
    )
    assert duplicate.status_code == 409
    # 历史分录保持原样，账目链完整
    ledger = client.get(f"/api/samples/{sample['id']}/ledger", headers=admin["headers"]).json()
    by_type = {entry["entry_type"]: entry for entry in ledger["entries"] if entry["entry_type"] != "reserve"}
    assert by_type["confirm"]["quantity_delta"] == -12
    assert by_type["correction_reversal"]["quantity_delta"] == 12
    assert by_type["correction_reentry"]["quantity_delta"] == -10
    assert ledger["totals"]["confirmed_consumption"] == 10


def test_correction_chain_on_reentry(client, admin):
    sample = create_sample(client, admin)
    reservation = reserve(client, admin, sample["id"], 30, "rsv-0014").json()["reservation"]
    confirm_entry = client.post(
        f"/api/samples/reservations/{reservation['id']}/confirm",
        headers=admin["headers"],
        json={"actual_quantity": 12, "idempotency_key": "cfm-0014"},
    ).json()["entries"][0]
    first = client.post(
        f"/api/samples/ledger-entries/{confirm_entry['id']}/corrections",
        headers=admin["headers"],
        json={"corrected_quantity": 10, "idempotency_key": "cor-0014a", "reason": "第一次更正"},
    ).json()
    second = client.post(
        f"/api/samples/ledger-entries/{first['reentry']['id']}/corrections",
        headers=admin["headers"],
        json={"corrected_quantity": 8, "idempotency_key": "cor-0014b", "reason": "第二次更正"},
    )
    assert second.status_code == 201, second.text
    assert second.json()["sample"]["quantity"] == 92
    ledger = client.get(f"/api/samples/{sample['id']}/ledger", headers=admin["headers"]).json()
    assert ledger["totals"]["confirmed_consumption"] == 8


def test_quarantine_expires_active_reservations(client, admin):
    sample = create_sample(client, admin)
    reservation = reserve(client, admin, sample["id"], 20, "rsv-0015").json()["reservation"]
    quarantined = client.post(
        f"/api/samples/{sample['id']}/quarantine", headers=admin["headers"], json={"reason": "检出污染"}
    )
    assert quarantined.status_code == 200, quarantined.text
    body = quarantined.json()
    assert body["sample"]["lifecycle_state"] == "quarantined"
    assert body["sample"]["reserved_quantity"] == 0
    assert [item["id"] for item in body["expired_reservations"]] == [reservation["id"]]
    detail = client.get(f"/api/samples/reservations/{reservation['id']}", headers=admin["headers"]).json()
    assert detail["reservation"]["state"] == "expired"
    assert detail["reservation"]["expiry_reason"] == "sample_quarantined"
    assert detail["entries"][-1]["entry_type"] == "expire"
    blocked = reserve(client, admin, sample["id"], 5, "rsv-0016")
    assert blocked.status_code == 409
    confirm_expired = client.post(
        f"/api/samples/reservations/{reservation['id']}/confirm",
        headers=admin["headers"],
        json={"actual_quantity": 1, "idempotency_key": "cfm-0015"},
    )
    assert confirm_expired.status_code == 409
    lifted = client.post(
        f"/api/samples/{sample['id']}/quarantine/lift", headers=admin["headers"], json={"reason": "复检合格"}
    )
    assert lifted.status_code == 200
    assert lifted.json()["sample"]["lifecycle_state"] == "available"


def _create_approver(client, admin, username):
    role = client.post(
        "/api/roles",
        headers=admin["headers"],
        json={"code": f"role-{username}", "name": f"审批人{username}", "permission_codes": ["samples.read", "approvals.decide"]},
    )
    assert role.status_code == 201, role.text
    user = client.post(
        "/api/users",
        headers=admin["headers"],
        json={"username": username, "password": "Approver!23456", "display_name": f"审批人{username}", "role_codes": [f"role-{username}"]},
    )
    assert user.status_code == 201, user.text
    login = client.post("/api/auth/login", json={"username": username, "password": "Approver!23456", "client_label": "tests"})
    assert login.status_code == 200
    return {"headers": {"Authorization": f"Bearer {login.json()['token']}"}, "id": user.json()["id"]}


def test_destruction_expires_active_reservations(client, admin):
    sample = create_sample(client, admin)
    reservation = reserve(client, admin, sample["id"], 20, "rsv-0017").json()["reservation"]
    approver_one = _create_approver(client, admin, "approver.one")
    approver_two = _create_approver(client, admin, "approver.two")
    approval = client.post(
        "/api/samples/approvals",
        headers=admin["headers"],
        json={"action_type": "destruction", "resource_type": "sample", "resource_id": sample["id"], "payload": {"quantity": 100}},
    )
    assert approval.status_code == 201, approval.text
    request_id = approval.json()["id"]
    for approver in (approver_one, approver_two):
        decision = client.post(
            f"/api/samples/approvals/{request_id}/decisions", headers=approver["headers"], json={"decision": "approve"}
        )
        assert decision.status_code == 200, decision.text
    destroyed = client.post(
        f"/api/sample-operations/destructions/{request_id}",
        headers=admin["headers"],
        json={"method": "高温焚烧", "witness_one": approver_one["id"], "witness_two": approver_two["id"]},
    )
    assert destroyed.status_code == 201, destroyed.text
    assert destroyed.json()["sample"]["lifecycle_state"] == "destroyed"
    detail = client.get(f"/api/samples/reservations/{reservation['id']}", headers=admin["headers"]).json()
    assert detail["reservation"]["state"] == "expired"
    assert detail["reservation"]["expiry_reason"] == "sample_destroyed"


def test_reconciliation_by_sample_and_experiment(client, admin):
    sample = create_sample(client, admin)
    other = create_sample(client, admin, sample_code="LED-002")
    first = reserve(client, admin, sample["id"], 30, "rsv-0020").json()["reservation"]
    reserve(client, admin, sample["id"], 10, "rsv-0021", experiment="EXP-OTHER")
    reserve(client, admin, other["id"], 7, "rsv-0022")
    client.post(
        f"/api/samples/reservations/{first['id']}/confirm",
        headers=admin["headers"],
        json={"actual_quantity": 12, "idempotency_key": "cfm-0020"},
    )
    ledger = client.get(f"/api/samples/{sample['id']}/ledger", headers=admin["headers"])
    assert ledger.status_code == 200, ledger.text
    totals = ledger.json()["totals"]
    assert totals["confirmed_consumption"] == 12
    assert totals["active_reserved"] == 10
    assert totals["reserved_delta_sum"] == 10
    assert totals["consistent"] is True
    experiment = client.get("/api/samples/ledger-entries", headers=admin["headers"], params={"experiment_code": "EXP-LEDGER"})
    assert experiment.status_code == 200, experiment.text
    body = experiment.json()
    assert body["totals"]["confirmed_consumption"] == 12
    assert body["totals"]["sample_codes"] == ["LED-001", "LED-002"]
    assert {entry["experiment_code"] for entry in body["entries"]} == {"EXP-LEDGER"}
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    event_types = [event["event_type"] for event in detail["events"]]
    assert "reservation.created" in event_types
    assert "reservation.confirmed" in event_types
    confirmed_event = next(event for event in detail["events"] if event["event_type"] == "reservation.confirmed")
    assert confirmed_event["details"]["experiment_code"] == "EXP-LEDGER"
    assert confirmed_event["quantity_delta"] == -12


def test_reserved_amount_blocks_other_stock_operations(client, admin):
    sample = create_sample(client, admin)
    assert reserve(client, admin, sample["id"], 80, "rsv-0030").status_code == 201
    # 直接消耗、借用、分装都只能动用未冻结的 20g
    consume = client.post(
        f"/api/samples/{sample['id']}/consumptions",
        headers=admin["headers"],
        json={"experiment_code": "EXP-LEGACY", "quantity": 30, "idempotency_key": "consume-0030"},
    )
    assert consume.status_code == 409
    loan = client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": sample["id"], "borrower_user_id": admin["body"]["user"]["id"], "quantity": 30, "due_at": "2026-10-01T00:00:00+00:00"},
    )
    assert loan.status_code == 409
    aliquot = client.post(
        f"/api/samples/{sample['id']}/aliquots",
        headers=admin["headers"],
        json={"requested_quantity": 30, "children": [{"sample_code": "LED-001-A", "quantity": 30}]},
    )
    assert aliquot.status_code == 409
    allowed = client.post(
        f"/api/samples/{sample['id']}/consumptions",
        headers=admin["headers"],
        json={"experiment_code": "EXP-LEGACY", "quantity": 20, "idempotency_key": "consume-0031"},
    )
    assert allowed.status_code == 201


def test_reserve_requires_consume_permission(client, admin):
    sample = create_sample(client, admin)
    role = client.post(
        "/api/roles",
        headers=admin["headers"],
        json={"code": "reader.only", "name": "只读", "permission_codes": ["samples.read"]},
    )
    assert role.status_code == 201
    user = client.post(
        "/api/users",
        headers=admin["headers"],
        json={"username": "reader.one", "password": "Reader!23456", "display_name": "只读用户", "role_codes": ["reader.only"]},
    )
    assert user.status_code == 201
    login = client.post("/api/auth/login", json={"username": "reader.one", "password": "Reader!23456", "client_label": "tests"})
    headers = {"Authorization": f"Bearer {login.json()['token']}"}
    denied = client.post(
        f"/api/samples/{sample['id']}/reservations",
        headers=headers,
        json={"experiment_code": "EXP-DENY", "quantity": 1, "idempotency_key": "rsv-deny"},
    )
    assert denied.status_code == 403
    allowed = client.get(f"/api/samples/{sample['id']}/ledger", headers=headers)
    assert allowed.status_code == 200
