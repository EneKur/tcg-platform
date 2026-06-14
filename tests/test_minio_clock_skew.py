"""MinIO clock-skew watchdog.

Runs scripts/check_minio_clock.sh and asserts the result is OK.
Warns on WARN. Skips on SKIP (MinIO unreachable). Fails on FAIL.

The script (not the test) is the source of truth for skew thresholds;
this test just glues the script into pytest.
"""
import subprocess
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "check_minio_clock.sh"


def _run_script() -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True, text=True, timeout=15,
    )


def test_minio_clock_skew_within_threshold():
    if not SCRIPT.exists():
        pytest.fail(f"watchdog script missing: {SCRIPT}")
    proc = _run_script()
    out = (proc.stdout or "").strip()
    if out.startswith("SKIP"):
        pytest.skip(out)
    assert proc.returncode == 0, f"clock check failed: {out!r}"
    if out.startswith("WARN"):
        with pytest.warns(UserWarning, match="MinIO clock skew"):
            import warnings
            warnings.warn(f"MinIO clock skew: {out}", UserWarning)
    assert out.startswith(("OK", "WARN")), f"unexpected output: {out!r}"
