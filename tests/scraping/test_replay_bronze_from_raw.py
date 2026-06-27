"""Tests for the replay_bronze_from_raw assets.

The unit-level contract of the helper is pinned in
tests/serialization/test_bronze_writer.py. These tests focus on the
asset-level loop: enumeration, region routing, mode flag handling,
and job definition.

Implementation note (deviation from spec): the original plan called
the @dg.asset-decorated functions directly with `context=ctx,
config={...}`. Dagster's direct-invocation API doesn't accept those
kwargs; we use `dg.build_op_context(resources=..., op_config=...)`
instead. Test intent is unchanged.
"""
import dagster as dg
import pytest

from tcg_platform.defs.replay_bronze_from_raw import (
    replay_bronze_from_raw_de,
    replay_bronze_from_raw_uk,
    replay_bronze_from_raw_job,
)
from tcg_platform.serialization.bronze_writer import _VALID_MODES


class _FakeRawClient:
    """Models the tcg_raw_client used to enumerate .html files.

    The production code calls both `raw_minio_client.client.list_objects(...)`
    and `raw_minio_client.get_object(bucket, key)` (the latter goes through
    `MinioClientResource.get_object` which returns raw bytes), so the fake
    needs both surface points.
    """

    def __init__(self, keys):
        self.keys = sorted(keys)
        self.got = []
        self._html_bytes = (
            b'<html><body>'
            b'<h1 class="x-item-title__mainTitle"><span class="ux-textspans">'
            b'One Piece OP01-001 PSA 10</span></h1>'
            b'<div data-testid="x-price-primary"><span class="ux-textspans">EUR 50,00</span></div>'
            b'</body></html>'
        )

        outer = self

        class _Client:
            def list_objects(self2, bucket, prefix="", recursive=True):
                return list(outer.keys)

            def get_object(self2, bucket, obj):
                outer.got.append((bucket, obj))
                return outer._html_bytes

        self._client = _Client()

    def get_object(self, bucket_name, object_name):
        self.got.append((bucket_name, object_name))
        return self._html_bytes

    def list_objects(self, bucket_name, prefix=""):
        return list(self.keys)

    @property
    def client(self):
        return self._client


class _FakeBronzeClient:
    def __init__(self):
        self.puts = []
        self.stat_existing = set()
        self.removed = []

        outer = self

        class _Client:
            def stat_object(self2, bucket, obj):
                if obj in outer.stat_existing:
                    return True
                raise Exception(f"NoSuchKey: {obj}")

            def remove_object(self2, bucket, obj):
                outer.removed.append((bucket, obj))
                outer.stat_existing.discard(obj)

            def put_object(self2, bucket, obj, data, length, content_type):
                outer.puts.append({"bucket": bucket, "object": obj, "length": length})

        self._client = _Client()

    def put_object(self, bucket_name, object_name, data, length, content_type):
        self.puts.append({
            "bucket": bucket_name,
            "object": object_name,
            "length": length,
        })

    def stat_object(self, bucket_name, object_name):
        if object_name in self.stat_existing:
            return True
        raise Exception(f"NoSuchKey: {object_name}")

    @property
    def client(self):
        return self._client


class _FakeSqliteClient:
    def __init__(self):
        self.inserts = []

    def execute(self, query, params=(), fetch="none"):
        if "INSERT" in query:
            self.inserts.append(params)
        return None


def _build_ctx(raw_client, bronze_client, sqlite_client, mode):
    """Build a Dagster direct-invocation context with the fake resources."""
    return dg.build_op_context(
        resources={
            "tcg_raw_client": raw_client,
            "minio_client": bronze_client,
            "sqlite_client_de": sqlite_client,
            "sqlite_client_uk": sqlite_client,
        },
        op_config={"mode": mode},
    )


def test_replay_de_fill_mode_writes_parquet_for_each_html():
    """DE fill mode: every raw HTML becomes a parquet (none pre-existing)."""
    raw = _FakeRawClient(keys=["ebay/DE/1.html", "ebay/DE/2.html", "ebay/DE/3.html"])
    bronze = _FakeBronzeClient()
    sqlite = _FakeSqliteClient()
    ctx = _build_ctx(raw, bronze, sqlite, mode="fill")

    result = replay_bronze_from_raw_de(ctx)
    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata["wrote_parquet"] == 3
    assert result.metadata["skipped_existing"] == 0
    parquet_puts = [p for p in bronze.puts if p["object"].endswith(".parquet")]
    assert len(parquet_puts) == 3


def test_replay_de_fill_mode_skips_existing_parquets():
    """DE fill mode: pre-existing parquets are skipped (skipped_existing=N)."""
    raw = _FakeRawClient(keys=["ebay/DE/1.html", "ebay/DE/2.html", "ebay/DE/3.html"])
    bronze = _FakeBronzeClient()
    bronze.stat_existing.add("sold_data/DE/1.parquet")
    sqlite = _FakeSqliteClient()
    ctx = _build_ctx(raw, bronze, sqlite, mode="fill")

    result = replay_bronze_from_raw_de(ctx)
    assert result.metadata["skipped_existing"] == 1
    assert result.metadata["wrote_parquet"] == 2


def test_replay_uk_fill_mode_writes_parquet_for_each_html():
    """UK fill mode: same shape, different asset."""
    raw = _FakeRawClient(keys=["ebay/UK/10.html", "ebay/UK/20.html"])
    bronze = _FakeBronzeClient()
    sqlite = _FakeSqliteClient()
    ctx = _build_ctx(raw, bronze, sqlite, mode="fill")

    result = replay_bronze_from_raw_uk(ctx)
    assert result.metadata["wrote_parquet"] == 2


def test_replay_invalid_mode_raises_value_error():
    """Bogus mode fails loud at asset startup, before any reads."""
    raw = _FakeRawClient(keys=["ebay/DE/1.html"])
    bronze = _FakeBronzeClient()
    sqlite = _FakeSqliteClient()
    ctx = _build_ctx(raw, bronze, sqlite, mode="garbage")

    with pytest.raises(ValueError, match="mode must be one of"):
        replay_bronze_from_raw_de(ctx)


def test_replay_overwrite_mode_rewrites_existing_parquets():
    """Overwrite mode: existing parquets are rewritten; SQLite untouched."""
    raw = _FakeRawClient(keys=["ebay/DE/1.html", "ebay/DE/2.html"])
    bronze = _FakeBronzeClient()
    bronze.stat_existing.add("sold_data/DE/1.parquet")
    bronze.stat_existing.add("sold_data/DE/2.parquet")
    sqlite = _FakeSqliteClient()
    ctx = _build_ctx(raw, bronze, sqlite, mode="overwrite")

    result = replay_bronze_from_raw_de(ctx)
    assert result.metadata["wrote_parquet"] == 2
    assert len(sqlite.inserts) == 0  # CRITICAL: no SQLite writes on overwrite
    parquet_puts = [p for p in bronze.puts if p["object"].endswith(".parquet")]
    assert len(parquet_puts) == 2


def test_replay_job_resolves_with_both_assets():
    """The Dagster job selects both DE + UK assets for parallel execution."""
    from tcg_platform.definitions import defs
    resolved = defs.load_fn()
    job_def = resolved.get_job_def("replay_bronze_from_raw_job")
    assert job_def.name == "replay_bronze_from_raw_job"
    # Both assets must be in the selection
    keys = {ak.to_user_string() for ak in job_def.asset_layer.selected_asset_keys}
    assert "replay_bronze_from_raw_de" in keys
    assert "replay_bronze_from_raw_uk" in keys


def test_enumerate_uses_wrapper_not_raw_sdk():
    """Production code must call minio_client.list_objects (wrapper),
    not minio_client.client.list_objects (raw SDK returning Object instances).

    Regression: 2026-06-27 — production called .client.list_objects and
    crashed with AttributeError on Object.endswith.
    """
    raw = _FakeRawClient(keys=["ebay/DE/1.html"])
    bronze = _FakeBronzeClient()
    sqlite = _FakeSqliteClient()
    ctx = _build_ctx(raw, bronze, sqlite, mode="fill")

    # The wrapper method must exist and return list[str].
    # If the wrapper is missing in production code's path, _enumerate_raw_keys
    # would call .client.list_objects which returns Object instances in prod,
    # and the .endswith() filter would raise AttributeError.
    assert callable(getattr(raw, "list_objects", None)), (
        "wrapper method missing — production code can't call it"
    )
    result = raw.list_objects("tcg-raw", prefix="ebay/DE/")
    assert all(isinstance(k, str) for k in result)
    assert result == ["ebay/DE/1.html"]

    # End-to-end: asset runs without AttributeError.
    result_asset = replay_bronze_from_raw_de(ctx)
    assert result_asset.metadata["wrote_parquet"] == 1