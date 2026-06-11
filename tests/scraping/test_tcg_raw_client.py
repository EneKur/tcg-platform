from tcg_platform.defs.minio_resources import tcg_raw_client


def test_tcg_raw_client_uses_raw_env_prefix(monkeypatch):
    """tcg_raw_client must read RAW_* env vars, not MINIO_*."""
    monkeypatch.setenv("RAW_ENDPOINT", "raw-host:9000")
    monkeypatch.setenv("RAW_ACCESS_KEY", "raw-key")
    monkeypatch.setenv("RAW_SECRET_KEY", "raw-secret")
    monkeypatch.setenv("RAW_BUCKET", "custom-raw")

    from tcg_platform.defs.minio_resources import _get_raw_config

    cfg = _get_raw_config()
    assert cfg == {
        "endpoint": "raw-host:9000",
        "access_key": "raw-key",
        "secret_key": "raw-secret",
        "bucket_name": "custom-raw",
        "secure": False,
    }


def test_tcg_raw_client_default_bucket_name(monkeypatch):
    """When RAW_BUCKET is unset, the raw resource defaults to 'tcg-raw'."""
    monkeypatch.delenv("RAW_BUCKET", raising=False)
    monkeypatch.setenv("RAW_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("RAW_ACCESS_KEY", "x")
    monkeypatch.setenv("RAW_SECRET_KEY", "y")
    from tcg_platform.defs.minio_resources import _get_raw_config

    cfg = _get_raw_config()
    assert cfg["bucket_name"] == "tcg-raw"


def test_minio_client_default_bucket_name_unchanged(monkeypatch):
    """Regression: default 'MINIO' prefix must still default to 'tcg-bronze'."""
    monkeypatch.delenv("MINIO_BUCKET", raising=False)
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    from tcg_platform.defs.minio_resources import _get_minio_config

    cfg = _get_minio_config(prefix="MINIO")
    assert cfg["bucket_name"] == "tcg-bronze"
