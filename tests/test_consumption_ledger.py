from __future__ import annotations

import threading

from app.core.security import Principal
from app.database import close_connection, get_connection, transaction
from app.samples.ledger import ConsumptionLedgerService

from tests.test_samples import create_batch_sample


def _reserve(client, admin, sample_id, experiment, quantity, key, **extra):
    payload = {"experiment_code": experiment, "quantity": quantity, "idempotency_key": key, "note": "预约"}
    payload.update(extra)
    return client.post(
        f"/api/samples/{sample_id}/consumption-reservations",
        headers=admin["headers"], json=payload,
    )


def test_reserve_freezes_available_without_touching_stock(client, admin):
    _, sample = create_batch_sample(client, admin)
    response = _reserve(client, admin, sample["id"], "EXP-100", 30, "rsv-100")
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["state"] == "reserved"
    assert body["reserved_quantity"] == 30
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["quantity"] == 100
    assert detail["reserved_quantity"] == 30


def test_concurrent_reservations_cannot_overdraw_sample(client, admin):
    _, sample = create_batch_sample(client, admin)
    # 串行视角：两次 60 的预约，总量 100，第二次必须被拒绝。
    first = _reserve(client, admin, sample["id"], "EXP-A", 60, "rsv-a")
    second = _reserve(client, admin, sample["id"], "EXP-B", 60, "rsv-b")
    assert first.status_code == 201
    assert second.status_code == 409
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["reserved_quantity"] == 60


def test_concurrent_threads_never_overdraw(client, admin):
    _, sample = create_batch_sample(client, admin)
    results: list[str] = []
    barrier = threading.Barrier(8)

    def worker(idx: int):
        try:
            conn = get_connection()
            principal = Principal(1, "admin", "平台主管", None, frozenset({"*"}), 1)
            barrier.wait()
            with transaction(immediate=True):
                ConsumptionLedgerService(conn).reserve(
                    principal, sample["id"],
                    {"experiment_code": f"EXP-T{idx}", "quantity": 20, "idempotency_key": f"rsv-t{idx}"},
                )
            results.append("ok")
        except Exception:  # noqa: BLE001 - 只统计成功数量
            results.append("fail")
        finally:
            close_connection()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results.count("ok") == 5  # 5 * 20 == 100，其余被透支保护拒绝
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["quantity"] == 100
    assert detail["reserved_quantity"] == 100


def test_confirm_settles_consumed_and_unused_in_one_go(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-200", 40, "rsv-200").json()
    response = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"],
        json={"actual_quantity": 30, "idempotency_key": "cfm-200", "note": "实际用 30"},
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["state"] == "confirmed"
    assert body["consumed_quantity"] == 30
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["quantity"] == 70
    assert detail["reserved_quantity"] == 0
    entry_types = sorted(entry["entry_type"] for entry in body["settled_entries"])
    assert entry_types == ["consume", "release"]


def test_confirm_zero_consumption_returns_everything(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-201", 15, "rsv-201").json()
    response = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json={"actual_quantity": 0},
    )
    assert response.status_code == 201, response.text
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["quantity"] == 100
    assert detail["reserved_quantity"] == 0
    assert response.json()["state"] == "confirmed"


def test_confirm_zero_consumption_replay(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-202", 15, "rsv-202").json()
    payload = {"actual_quantity": 0, "idempotency_key": "cfm-202"}
    first = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json=payload,
    )
    second = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json=payload,
    )
    assert first.status_code == second.status_code == 201
    assert second.json()["replayed"] is True
    assert second.json()["replayed_entry"]["operation"] == "confirm"


def test_correction_replay_returns_original_result(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-403", 20, "rsv-403").json()
    client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json={"actual_quantity": 10},
    )
    payload = {"correct_quantity": 12, "reason": "补录两单位", "idempotency_key": "cor-403"}
    first = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/corrections",
        headers=admin["headers"], json=payload,
    )
    second = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/corrections",
        headers=admin["headers"], json=payload,
    )
    assert first.status_code == second.status_code == 201
    assert second.json()["replayed"] is True
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["quantity"] == 88  # 只更正一次


def test_correction_from_zero_original(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-404", 20, "rsv-404").json()
    client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json={"actual_quantity": 0},
    )
    correction = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/corrections",
        headers=admin["headers"],
        json={"correct_quantity": 5, "reason": "零消耗确认后补录实际使用", "idempotency_key": "cor-404"},
    )
    assert correction.status_code == 201, correction.text
    assert correction.json()["reversal_entries"] == []
    assert correction.json()["new_entries"][0]["quantity"] == 5
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["quantity"] == 95


def test_legacy_direct_consumption_also_posts_to_ledger(client, admin):
    _, sample = create_batch_sample(client, admin)
    response = client.post(
        f"/api/samples/{sample['id']}/consumptions", headers=admin["headers"],
        json={"experiment_code": "EXP-LEGACY", "quantity": 12.5,
              "idempotency_key": "legacy-001", "note": "旧接口直扣"},
    )
    assert response.status_code == 201, response.text
    ledger = client.get(
        "/api/samples/consumption-ledger", headers=admin["headers"],
        params={"experiment_code": "EXP-LEGACY"},
    ).json()
    assert len(ledger["entries"]) == 1
    entry = ledger["entries"][0]
    assert entry["operation"] == "direct"
    assert entry["on_hand_delta"] == -12.5


def test_release_unfreezes_on_cancellation(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-300", 25, "rsv-300").json()
    response = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/release",
        headers=admin["headers"], json={"idempotency_key": "rel-300", "note": "实验取消"},
    )
    assert response.status_code == 201, response.text
    assert response.json()["state"] == "released"
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["quantity"] == 100
    assert detail["reserved_quantity"] == 0
    # 已释放的预约不能再确认或重复释放。
    again = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/release",
        headers=admin["headers"], json={},
    )
    assert again.status_code == 409


def test_correction_uses_reversal_and_new_entries_without_overwriting(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-400", 50, "rsv-400").json()
    client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json={"actual_quantity": 40},
    )
    ledger_before = client.get(
        "/api/samples/consumption-ledger", headers=admin["headers"],
        params={"experiment_code": "EXP-400"},
    ).json()
    original_entries = ledger_before["entries"]

    correction = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/corrections",
        headers=admin["headers"],
        json={"correct_quantity": 45, "reason": "录少了，实际 45"},
    )
    assert correction.status_code == 201, correction.text
    body = correction.json()
    assert body["consumed_quantity"] == 45
    assert body["reversal_entries"][0]["entry_type"] == "consume_reversal"
    assert body["reversal_entries"][0]["reversal_of_id"] is not None
    assert body["new_entries"][0]["entry_type"] == "consume"
    assert body["new_entries"][0]["quantity"] == 45
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["quantity"] == 55  # 100 - 45
    # 历史分录原封不动，只是新增红冲与新分录。
    ledger_after = client.get(
        "/api/samples/consumption-ledger", headers=admin["headers"],
        params={"experiment_code": "EXP-400"},
    ).json()
    assert ledger_after["entries"][: len(original_entries)] == original_entries
    assert len(ledger_after["entries"]) == len(original_entries) + 2


def test_repeated_corrections_chain_reversals(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-401", 50, "rsv-401").json()
    client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json={"actual_quantity": 40},
    )
    client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/corrections",
        headers=admin["headers"], json={"correct_quantity": 45, "reason": "第一次更正"},
    )
    second = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/corrections",
        headers=admin["headers"], json={"correct_quantity": 30, "reason": "第二次更正"},
    )
    assert second.status_code == 201, second.text
    # 第二次红冲针对当前生效的 45，而不是最初的 40。
    assert second.json()["reversal_entries"][0]["quantity"] == 45
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["quantity"] == 70


def test_correction_validations(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-402", 10, "rsv-402").json()
    # 未确认不能更正。
    refused = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/corrections",
        headers=admin["headers"], json={"correct_quantity": 5, "reason": "试图改未确认"},
    )
    assert refused.status_code == 409
    client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json={"actual_quantity": 8},
    )
    same = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/corrections",
        headers=admin["headers"], json={"correct_quantity": 8, "reason": "数量没变"},
    )
    assert same.status_code == 422


def test_reserve_replay_returns_original_result(client, admin):
    _, sample = create_batch_sample(client, admin)
    payload = {"experiment_code": "EXP-500", "quantity": 20, "idempotency_key": "rsv-500"}
    first = client.post(f"/api/samples/{sample['id']}/consumption-reservations", headers=admin["headers"], json=payload)
    second = client.post(f"/api/samples/{sample['id']}/consumption-reservations", headers=admin["headers"], json=payload)
    assert first.status_code == second.status_code == 201
    assert first.json()["id"] == second.json()["id"]
    assert second.json()["replayed"] is True
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["reserved_quantity"] == 20  # 没有二次冻结


def test_same_idempotency_key_different_payload_rejected(client, admin):
    _, sample = create_batch_sample(client, admin)
    _reserve(client, admin, sample["id"], "EXP-501", 20, "shared-key")
    conflict = _reserve(client, admin, sample["id"], "EXP-501", 21, "shared-key")
    assert conflict.status_code == 409


def test_confirm_replay_is_idempotent(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-502", 30, "rsv-502").json()
    payload = {"actual_quantity": 12, "idempotency_key": "cfm-502"}
    first = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json=payload,
    )
    second = client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json=payload,
    )
    assert first.status_code == second.status_code == 201
    assert second.json()["replayed"] is True
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["quantity"] == 88


def test_quarantine_invalidates_only_unconfirmed_reservations(client, admin):
    _, sample = create_batch_sample(client, admin)
    confirmed = _reserve(client, admin, sample["id"], "EXP-600", 20, "rsv-600").json()
    open_reservation = _reserve(client, admin, sample["id"], "EXP-601", 30, "rsv-601").json()
    client.post(
        f"/api/samples/consumption-reservations/{confirmed['id']}/confirm",
        headers=admin["headers"], json={"actual_quantity": 20},
    )
    response = client.post(
        f"/api/samples/{sample['id']}/quarantine",
        headers=admin["headers"], json={"reason": "疑似污染，隔离待查"},
    )
    assert response.status_code == 201, response.text
    invalidated = response.json()["invalidated_reservations"]
    assert [item["id"] for item in invalidated] == [open_reservation["id"]]
    assert invalidated[0]["invalidated_reason"] == "sample_quarantined"
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert detail["lifecycle_state"] == "quarantined"
    assert detail["reserved_quantity"] == 0
    r_confirmed = client.get(
        f"/api/samples/consumption-reservations/{confirmed['id']}", headers=admin["headers"]
    ).json()
    assert r_confirmed["state"] == "confirmed"
    # 隔离后不能再预约，失效预约不能确认。
    assert _reserve(client, admin, sample["id"], "EXP-602", 1, "rsv-602").status_code == 409
    confirm = client.post(
        f"/api/samples/consumption-reservations/{open_reservation['id']}/confirm",
        headers=admin["headers"], json={"actual_quantity": 1},
    )
    assert confirm.status_code == 409
    # 重放隔离请求保持幂等。
    replay = client.post(
        f"/api/samples/{sample['id']}/quarantine",
        headers=admin["headers"], json={"reason": "再次调用"},
    )
    assert replay.json()["replayed"] is True


def test_researcher_can_reserve_but_not_quarantine(client, admin):
    _, sample = create_batch_sample(client, admin)
    user = client.post(
        "/api/users", headers=admin["headers"],
        json={"username": "researcher.ledger", "password": "Research!23456",
              "display_name": "账本研究员", "role_codes": ["researcher"]},
    )
    assert user.status_code == 201, user.text
    login = client.post(
        "/api/auth/login",
        json={"username": "researcher.ledger", "password": "Research!23456", "client_label": "tests"},
    ).json()
    headers = {"Authorization": f"Bearer {login['token']}"}
    ok = _reserve(client, admin, sample["id"], "EXP-700", 10, "rsv-700")
    # 用研究人员身份直接预约。
    response = client.post(
        f"/api/samples/{sample['id']}/consumption-reservations", headers=headers,
        json={"experiment_code": "EXP-701", "quantity": 5, "idempotency_key": "rsv-701"},
    )
    assert response.status_code == 201, response.text
    denied = client.post(
        f"/api/samples/{sample['id']}/quarantine", headers=headers, json={"reason": "无权隔离"}
    )
    assert denied.status_code == 403
    assert ok.status_code == 201


def test_ledger_reconciles_by_sample_event_and_experiment(client, admin):
    _, sample = create_batch_sample(client, admin)
    reservation = _reserve(client, admin, sample["id"], "EXP-800", 40, "rsv-800").json()
    client.post(
        f"/api/samples/consumption-reservations/{reservation['id']}/confirm",
        headers=admin["headers"], json={"actual_quantity": 35},
    )
    # 从实验编号对账。
    by_experiment = client.get(
        "/api/samples/consumption-ledger", headers=admin["headers"],
        params={"experiment_code": "EXP-800"},
    ).json()
    assert [entry["entry_type"] for entry in by_experiment["entries"]] == ["reserve", "consume", "release"]
    assert by_experiment["totals"] == {"on_hand_delta": -35.0, "frozen_delta": 0.0}
    # 样品事件携带账本分录编号，可从样品事件追溯到每次库存变化。
    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    confirmed_event = next(event for event in detail["events"] if event["event_type"] == "consumption.confirmed")
    assert len(confirmed_event["details"]["ledger_entry_ids"]) == 2
    # 实验编号也能列出预约。
    reservations = client.get(
        "/api/samples/experiments/EXP-800/consumption-reservations", headers=admin["headers"]
    ).json()["reservations"]
    assert [item["id"] for item in reservations] == [reservation["id"]]


def _approver_login(client, admin, username):
    role = client.post(
        "/api/roles", headers=admin["headers"],
        json={"code": f"role.{username}", "name": f"审批人 {username}",
              "permission_codes": ["approvals.decide", "samples.read", "samples.destroy"]},
    )
    assert role.status_code == 201, role.text
    user = client.post(
        "/api/users", headers=admin["headers"],
        json={"username": username, "password": "Approver!23456",
              "display_name": username, "role_codes": [f"role.{username}"]},
    )
    assert user.status_code == 201, user.text
    login = client.post(
        "/api/auth/login",
        json={"username": username, "password": "Approver!23456", "client_label": "tests"},
    ).json()
    return user.json()["id"], {"Authorization": f"Bearer {login['token']}"}


def test_destruction_invalidates_open_reservations_end_to_end(client, admin):
    _, sample = create_batch_sample(client, admin)
    open_reservation = _reserve(client, admin, sample["id"], "EXP-900", 40, "rsv-900").json()
    approver_one_id, headers_one = _approver_login(client, admin, "approver.one")
    approver_two_id, headers_two = _approver_login(client, admin, "approver.two")
    approval = client.post(
        "/api/samples/approvals", headers=admin["headers"],
        json={"action_type": "destruction", "resource_type": "sample",
              "resource_id": sample["id"], "payload": {"quantity": 100}},
    ).json()
    client.post(
        f"/api/samples/approvals/{approval['id']}/decisions",
        headers=headers_one, json={"decision": "approve"},
    )
    client.post(
        f"/api/samples/approvals/{approval['id']}/decisions",
        headers=headers_two, json={"decision": "approve"},
    )
    executed = client.post(
        f"/api/sample-operations/destructions/{approval['id']}",
        headers=admin["headers"],
        json={"method": "高压灭菌", "witness_one": approver_one_id, "witness_two": approver_two_id},
    )
    assert executed.status_code == 201, executed.text
    assert executed.json()["sample"]["lifecycle_state"] == "destroyed"
    reservation = client.get(
        f"/api/samples/consumption-reservations/{open_reservation['id']}", headers=admin["headers"]
    ).json()
    assert reservation["state"] == "invalidated"
    assert reservation["invalidated_reason"] == "sample_destroyed"
    ledger = client.get(
        "/api/samples/consumption-ledger", headers=admin["headers"],
        params={"experiment_code": "EXP-900"},
    ).json()
    assert [entry["entry_type"] for entry in ledger["entries"]] == ["reserve", "invalidate"]
