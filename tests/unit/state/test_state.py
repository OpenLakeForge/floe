"""Ported from `crates/floe-core/tests/unit/state/mod.rs` (state cases)."""

import io
import json
from pathlib import Path

import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber

import floe.state as state_mod
from floe.errors import FloeConfigError
from floe.state import (
    SCHEMA_V1,
    SCHEMA_V2,
    Claim,
    EntityState,
    FileState,
    claim_all,
    claim_inputs,
    dump_state,
    promote,
    read_state,
    release,
    renew,
    reset,
)
from floe.storage.inputs import InputFile
from floe.storage.local import LocalClient
from floe.storage.s3 import S3Client
from floe.storage.types import StoredObject

FIXTURE = Path(__file__).parent / "rust_0_7_state.json"
URI = "local:///work/in/orders.csv"


def _no_backoff(attempt: int) -> None:
    return None


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(state_mod, "_backoff", _no_backoff)


def _input(uri: str = "local:///tmp/incoming/sales.csv") -> InputFile:
    return InputFile(uri, "sales.csv", "sales", 42, "2026-04-22T08:00:00Z")


def _claim(run_id: str, expires_at: str) -> Claim:
    return Claim(run_id, "2000-01-01T00:00:00Z", expires_at, 42, None)


def test_writes_and_reads_entity_state(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "incoming/.floe/state/sales/state.json"), LocalClient()
    files = {
        "local:///tmp/sales.csv": FileState(
            "2026-04-20T12:59:14Z", 182731, "2026-04-20T11:30:00Z"
        )
    }
    state = EntityState("sales", "2026-04-20T13:00:00Z", files, {})
    assert client.write_conditional(uri, None, dump_state(state)) is not None
    assert read_state(client, uri, "sales") == state
    assert len(list(Path(uri).parent.iterdir())) == 1


def test_read_state_returns_none_when_missing(tmp_path: Path) -> None:
    assert read_state(LocalClient(), str(tmp_path / "missing/state.json"), "x") is None


def test_v1_state_is_upgraded_and_claims_dropped(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    doc = {
        "schema": SCHEMA_V1,
        "entity": "sales",
        "updated_at": None,
        "files": {"a": {"processed_at": "t", "size": None, "mtime": None}},
        "claims": {"b": asdict_claim()},
    }
    client.write_conditional(uri, None, json.dumps(doc).encode())
    state = read_state(client, uri, "sales")
    expected = EntityState("sales", None, {"a": FileState("t", None, None)}, {})
    assert state == expected
    assert json.loads(dump_state(expected))["schema"] == SCHEMA_V2


def test_omitted_optional_fields_default_to_none(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    doc = {
        "schema": SCHEMA_V2,
        "entity": "sales",
        "files": {"a": {"processed_at": "t"}},
        "claims": {"b": {"run_id": "r", "acquired_at": "a", "expires_at": "e"}},
    }
    client.write_conditional(uri, None, json.dumps(doc).encode())
    expected = EntityState(
        "sales", None, {"a": FileState("t")}, {"b": Claim("r", "a", "e")}
    )
    assert read_state(client, uri, "sales") == expected


def asdict_claim() -> dict[str, object]:
    return {
        "run_id": "r",
        "acquired_at": "a",
        "expires_at": "e",
        "size": 1,
        "mtime": None,
    }


def test_reads_state_written_by_rust_0_7() -> None:
    body = FIXTURE.read_bytes()
    stored = StoredObject(body, "v")

    class One(LocalClient):
        def read(self, uri: str) -> StoredObject | None:
            return stored

    state = read_state(One(), "x", "orders")
    assert state is not None
    assert state.entity == "orders"
    assert state.claims == {}
    file = state.files["local:///work/in/orders.csv"]
    assert file.size == 16
    assert file.processed_at.endswith("Z")
    assert dump_state(state) == body


@pytest.mark.parametrize(
    ("doc", "message"),
    [
        (
            {"schema": "bad", "entity": "sales", "updated_at": None},
            f"entity.name=sales state schema mismatch: expected {SCHEMA_V1} or {SCHEMA_V2}, got bad",
        ),
        (
            {"schema": SCHEMA_V2, "entity": "other", "updated_at": None},
            "entity.name=sales state entity mismatch: expected sales, got other",
        ),
    ],
)
def test_state_validation_messages(
    tmp_path: Path, doc: dict[str, str | None], message: str
) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    client.write_conditional(uri, None, json.dumps(doc).encode())
    with pytest.raises(FloeConfigError) as e:
        read_state(client, uri, "sales")
    assert str(e.value) == message


def test_claim_skips_active_claims_from_other_runs(tmp_path: Path) -> None:
    uri, client, f = str(tmp_path / "state.json"), LocalClient(), _input()
    first = claim_inputs(client, uri, "sales", "run-a", [f])
    assert first.pending == [f]
    assert first.claimed is not None
    second = claim_inputs(client, uri, "sales", "run-b", [f])
    assert second.pending == []
    assert second.active_claims == [f.source_uri]
    state = read_state(client, uri, "sales")
    assert state is not None
    assert state.files == {}
    assert {u: c.run_id for u, c in state.claims.items()} == {f.source_uri: "run-a"}


def test_claim_removes_expired_claims_before_acquiring(tmp_path: Path) -> None:
    uri, client, f = str(tmp_path / "state.json"), LocalClient(), _input()
    old = EntityState(
        "sales", None, {}, {f.source_uri: _claim("crashed", "2000-01-01T01:00:00Z")}
    )
    client.write_conditional(uri, None, dump_state(old))
    outcome = claim_inputs(client, uri, "sales", "run-b", [f])
    assert outcome.pending == [f]
    assert outcome.active_claims == []
    state = read_state(client, uri, "sales")
    assert state is not None
    assert [c.run_id for c in state.claims.values()] == ["run-b"]


def test_claim_with_unparsable_expiry_is_treated_as_expired(tmp_path: Path) -> None:
    uri, client, f = str(tmp_path / "state.json"), LocalClient(), _input()
    old = EntityState("sales", None, {}, {f.source_uri: _claim("x", "not-a-date")})
    client.write_conditional(uri, None, dump_state(old))
    assert claim_inputs(client, uri, "sales", "run-b", [f]).pending == [f]


def test_claim_reports_already_processed(tmp_path: Path) -> None:
    uri, client, f = str(tmp_path / "state.json"), LocalClient(), _input()
    done = FileState("t", 42, None)
    client.write_conditional(
        uri, None, dump_state(EntityState("sales", None, {f.source_uri: done}, {}))
    )
    outcome = claim_inputs(client, uri, "sales", "run-a", [f])
    assert outcome.pending == []
    assert outcome.claimed is None
    assert outcome.already_processed == [(f, done)]


def test_claim_with_no_inputs_touches_nothing(tmp_path: Path) -> None:
    uri = str(tmp_path / "state.json")
    assert claim_inputs(LocalClient(), uri, "sales", "run-a", []).claimed is None
    assert not Path(uri).exists()


def test_promote_moves_own_claims_to_files(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    a, b = _input("a"), _input("b")
    claim_inputs(client, uri, "sales", "run-b", [b])
    outcome = claim_inputs(client, uri, "sales", "run-a", [a])
    assert outcome.claimed is not None
    promote(client, uri, "run-a", outcome.claimed)
    state = read_state(client, uri, "sales")
    assert state is not None
    assert state.files["a"].size == 42
    assert list(state.claims) == ["b"]
    again = claim_inputs(client, uri, "sales", "run-c", [a])
    assert again.pending == [] and again.already_processed[0][0] == a


def test_release_drops_own_claims_only(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    claim_inputs(client, uri, "sales", "run-b", [_input("b")])
    outcome = claim_inputs(client, uri, "sales", "run-a", [_input("a")])
    assert outcome.claimed is not None
    release(client, uri, "run-a", outcome.claimed)
    state = read_state(client, uri, "sales")
    assert state is not None
    assert list(state.claims) == ["b"]


def test_renew_extends_own_claims(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    outcome = claim_inputs(client, uri, "sales", "run-a", [_input("a")])
    assert outcome.claimed is not None
    outcome.claimed.state.claims["a"].expires_at = "2000-01-01T00:00:00Z"
    renew(client, uri, "run-a", outcome.claimed)
    state = read_state(client, uri, "sales")
    assert state is not None
    assert state.claims["a"].expires_at > "2000-01-01T00:00:00Z"


class Flaky(LocalClient):
    """Loses the first `conflicts` conditional writes, as a concurrent writer would."""

    def __init__(self, conflicts: int) -> None:
        self.conflicts = conflicts

    def write_conditional(
        self, uri: str, expected_version: str | None, body: bytes
    ) -> str | None:
        if self.conflicts:
            self.conflicts -= 1
            return None
        return super().write_conditional(uri, expected_version, body)


def test_claim_retries_on_cas_conflict(tmp_path: Path) -> None:
    uri = str(tmp_path / "state.json")
    outcome = claim_inputs(Flaky(3), uri, "sales", "run-a", [_input()])
    assert len(outcome.pending) == 1


def test_claim_fails_after_retries(tmp_path: Path) -> None:
    uri = str(tmp_path / "state.json")
    with pytest.raises(FloeConfigError) as e:
        claim_inputs(Flaky(99), uri, "sales", "run-a", [_input()])
    assert (
        str(e.value)
        == "entity.name=sales incremental state update conflicted after 8 retries"
    )


def test_promote_reloads_after_conflict(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    outcome = claim_inputs(client, uri, "sales", "run-a", [_input("a")])
    assert outcome.claimed is not None
    promote(Flaky(2), uri, "run-a", outcome.claimed)
    state = read_state(client, uri, "sales")
    assert state is not None
    assert list(state.files) == ["a"]


def test_release_of_the_only_claim_deletes_the_state(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    outcome = claim_inputs(client, uri, "sales", "run-a", [_input("a")])
    assert outcome.claimed is not None
    release(client, uri, "run-a", outcome.claimed)
    assert not Path(uri).exists()


def test_full_refresh_claims_all_and_promote_keeps_only_this_run(
    tmp_path: Path,
) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    first = claim_inputs(client, uri, "sales", "run-a", [_input("a"), _input("b")])
    assert first.claimed is not None
    promote(client, uri, "run-a", first.claimed)
    claim_inputs(client, uri, "sales", "run-x", [_input("c")])
    claimed = claim_all(client, uri, "sales", "run-b", [_input("a")])
    stored = read_state(client, uri, "sales")
    assert stored is not None
    assert (sorted(stored.files), list(stored.claims)) == (["a", "b"], ["a"])
    promote(client, uri, "run-b", claimed, full_refresh=True)
    stored = read_state(client, uri, "sales")
    assert stored is not None
    assert (list(stored.files), stored.claims) == (["a"], {})


def test_full_refresh_with_no_inputs_deletes_the_state(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    first = claim_inputs(client, uri, "sales", "run-a", [_input("a")])
    assert first.claimed is not None
    promote(client, uri, "run-a", first.claimed)
    promote(client, uri, "run-b", claim_all(client, uri, "sales", "run-b", []), True)
    assert not Path(uri).exists()


def test_claim_all_fails_after_retries(tmp_path: Path) -> None:
    with pytest.raises(FloeConfigError, match="full-refresh state write conflicted after 8 retries"):  # fmt: skip
        claim_all(Flaky(99), str(tmp_path / "state.json"), "sales", "run-a", [_input()])


@pytest.mark.parametrize(
    "body",
    [
        dump_state(EntityState("sales", None, {}, {})),
        b"{not valid json",
        b'{"schema":"wrong.schema","entity":"other","updated_at":null,"files":{}}',
    ],
)
def test_reset_removes_existing_malformed_or_mismatched_state(
    tmp_path: Path, body: bytes
) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    client.write_conditional(uri, None, body)
    assert reset(client, uri, "sales") is True
    assert not Path(uri).exists()
    assert reset(client, uri, "sales") is False


def test_reset_raises_when_state_changes_meanwhile(tmp_path: Path) -> None:
    uri = str(tmp_path / "state.json")

    class Racing(LocalClient):
        def delete_conditional(self, uri: str, expected_version: str | None) -> bool:
            return False

    LocalClient().write_conditional(uri, None, b"{}")
    with pytest.raises(FloeConfigError) as err:
        reset(Racing(), uri, "sales")
    assert str(err.value) == f"entity.name=sales remote state changed while resetting: {uri}"  # fmt: skip


def test_reset_deletes_s3_state_with_its_etag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    client = S3Client("b", "us-east-1")
    key = {"Bucket": "b", "Key": "s/state.json"}
    with Stubber(client._client) as stub:  # pyright: ignore[reportPrivateUsage]
        body = StreamingBody(io.BytesIO(b"{not valid json"), 15)
        stub.add_response("get_object", {"ETag": '"e1"', "Body": body}, key)  # pyright: ignore[reportUnknownMemberType]
        stub.add_response("delete_object", {}, key | {"IfMatch": '"e1"'})  # pyright: ignore[reportUnknownMemberType]
        stub.add_client_error(
            "get_object", "NoSuchKey", http_status_code=404, expected_params=key
        )  # pyright: ignore[reportUnknownMemberType]
        assert reset(client, "s3://b/s/state.json", "sales") is True
        assert reset(client, "s3://b/s/state.json", "sales") is False
        stub.assert_no_pending_responses()
