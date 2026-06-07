# Limitless Card Image Sync — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a single-button Dagster job (`sync_card_images_job`) that keeps `tcg-bronze/cards/` in sync with the current Limitless TCG card catalog — discovers every card (base + `?v=N` variants), diffs against MinIO, downloads only missing webp files.

**Architecture:** Two Dagster assets in one job. `discover_limitless_catalog` Playwright-scrapes Limitless's `/cards/` index and per-set pages → returns a list of `(set_code, card_id, variant)` tuples. `sync_card_images` does one `list_objects("tcg-bronze", "cards/")` call, in-memory diffs, and CDN-fetches only the missing cards. New job registered alongside `complete_eu_pipeline` in `definitions.py`.

**Tech Stack:** Dagster, Playwright, BeautifulSoup, requests, MinIO Python client, pytest.

**Spec:** `docs/superpowers/specs/2026-06-07-limitless-card-image-sync-design.md`

---

## File Structure

**New files:**
- `src/tcg_platform/defs/discover_limitless_catalog.py` — Dagster asset, Playwright orchestration only (no business logic).
- `src/tcg_platform/defs/sync_card_images.py` — Dagster asset, CDN fetch + put_object orchestration only.
- `tests/scraping/test_extract_card_links_from_set_page.py` — TDD tests for the parser extracted from `limitlesstcg.py`.
- `tests/scraping/test_sync_card_diff.py` — TDD tests for the pure diff function.

**Modified files:**
- `src/tcg_platform/scraping/limitlesstcg.py` — extract `extract_card_links_from_set_page()` pure function; add `P` to prefix set; refactor `scrape_limitless_op` to call the new function.
- `src/tcg_platform/definitions.py` — register `sync_card_images_job` and add to `jobs=[...]` list.
- `log/SESSION_2026-06-07.md` — session log per AGENTS.md.

The two Dagster assets are kept thin. All parsing and diff logic lives in pure module-level functions, which is what the existing test files (e.g. `test_silver_file_writer.py`) do — test the logic, not the asset wrapper.

---

## Task 1: Extract `extract_card_links_from_set_page` from `limitlesstcg.py`

**Files:**
- Modify: `src/tcg_platform/scraping/limitlesstcg.py` (extract function, refactor `scrape_limitless_op`)
- Create: `tests/scraping/test_extract_card_links_from_set_page.py`

- [ ] **Step 1: Write the failing test**

Create `tests/scraping/test_extract_card_links_from_set_page.py`:

```python
from tcg_platform.scraping.limitlesstcg import extract_card_links_from_set_page


SET_PAGE_HTML = """
<html><body>
  <a href="/cards/op01-001">Luffy</a>
  <a href="/cards/op01-002">Zoro</a>
  <a href="/cards/op01-001?v=1">Luffy alt</a>
  <a href="/cards/op01-001?v=2">Luffy manga</a>
  <a href="/cards/p-001">Promo Luffy</a>
  <a href="/cards/leader-some-event">Ignore me</a>  <!-- not a card link -->
  <a href="/something/else">Ignore</a>
  <a href="/cards/op01-001">Luffy</a>  <!-- duplicate, dedupe -->
</body></html>
"""


def test_parses_base_cards():
    result = extract_card_links_from_set_page(SET_PAGE_HTML)
    base_cards = [c for (c, v) in result if v is None]
    assert "OP01-001" in base_cards
    assert "OP01-002" in base_cards
    assert "P-001" in base_cards  # P prefix is included (fixes existing bug)


def test_parses_variants():
    result = extract_card_links_from_set_page(SET_PAGE_HTML)
    variants = {c: v for (c, v) in result if v is not None}
    assert variants == {"OP01-001": 1} or "OP01-001" in variants
    # OP01-001 has v=1 and v=2 → both should appear
    op001_variants = sorted([v for (c, v) in result if c == "OP01-001" and v is not None])
    assert op001_variants == [1, 2]


def test_dedupes_duplicate_links():
    result = extract_card_links_from_set_page(SET_PAGE_HTML)
    # OP01-001 base appears twice in HTML; should appear once in result
    base_op001 = [1 for (c, v) in result if c == "OP01-001" and v is None]
    assert sum(base_op001) == 1


def test_empty_html():
    assert extract_card_links_from_set_page("<html><body></body></html>") == []


def test_no_variants_returns_none_variant():
    html = """<html><body><a href="/cards/op01-001">x</a><a href="/cards/op01-002">y</a></body></html>"""
    result = extract_card_links_from_set_page(html)
    assert result == [("OP01-001", None), ("OP01-002", None)]


def test_card_id_uppercased():
    """Limitless URLs are lowercase; output card_ids must be uppercase."""
    html = '<html><body><a href="/cards/op01-001">x</a></body></html>'
    result = extract_card_links_from_set_page(html)
    assert result == [("OP01-001", None)]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/pytest tests/scraping/test_extract_card_links_from_set_page.py -v`
Expected: FAIL with `ImportError: cannot import name 'extract_card_links_from_set_page'`

- [ ] **Step 3: Implement the function**

In `src/tcg_platform/scraping/limitlesstcg.py`, add the new function at module level (place it after `_get_all_sets`):

```python
def extract_card_links_from_set_page(html: str) -> list[tuple[str, int | None]]:
    """Parse a Limitless set page; return [(card_id, variant), ...] deduped.

    card_id is uppercased. variant is None for base cards, int for ?v=N printings.
    """
    soup = BeautifulSoup(html, "html.parser")
    set_prefixes = ("OP", "EB", "ST", "PR", "P")
    raw = [
        a.get("href")
        for a in soup.find_all("a")
        if any(f"/cards/{p}" in (a.get("href") or "").upper() for p in set_prefixes)
    ]
    out: list[tuple[str, int | None]] = []
    seen: set[tuple[str, int | None]] = set()
    for href in raw:
        path, _, query = href.partition("?")
        card_id = path.rsplit("/", 1)[-1].upper()
        variant: int | None = None
        if query:
            for part in query.split("&"):
                if part.startswith("v="):
                    try:
                        variant = int(part[2:])
                    except ValueError:
                        variant = None
        key = (card_id, variant)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out
```

- [ ] **Step 4: Refactor `scrape_limitless_op` to use it**

In `src/tcg_platform/scraping/limitlesstcg.py`, replace the inline `card_links` extraction (lines 154-155) with a call to the new function. The block to replace is:

```python
            card_links = [a.get("href") for a in soup.find_all("a") if "/cards/OP" in a.get("href", "") or "/cards/EB" in a.get("href", "") or "/cards/PR" in a.get("href", "") or "/cards/ST" in a.get("href", "")]
            card_links = list(set(card_links))
```

Replace with:

```python
            card_links = extract_card_links_from_set_page(html)
```

Then update the loop below it. The current loop iterates `for card_link in card_links` where each `card_link` is a URL string like `/cards/op01-001`. After the refactor, `card_links` is a list of `(card_id, variant)` tuples. The new loop should be:

```python
            for card_id, variant in card_links:
                card_url = f"{LIMITLESS_OP_BASE}/cards/{card_id.lower()}"
                try:
                    card_page = browser.new_page()
                    card_page.goto(card_url, timeout=60000)
                    card_page.wait_for_load_state("networkidle", timeout=30000)

                    card_html = card_page.content()
                    card_record, price_records = _parse_card_page(card_html, set_code, scraped_at)

                    if card_record:
                        all_cards.append(card_record)
                        all_prices.extend(price_records)

                    card_page.close()
                except Exception as e:
                    print(f"Error scraping {card_id}: {e}")
                    continue
```

Note: variants (`?v=1`) are not currently fetched by `scrape_limitless_op` (the original code only handled base links) and remain so. This preserves existing behavior; variant coverage is the new sync asset's job.

- [ ] **Step 5: Run tests to verify all pass**

Run: `.venv/bin/pytest tests/scraping/test_extract_card_links_from_set_page.py tests/scraping/ -v`
Expected: All new tests pass; existing tests still pass. If any test_exchange_rate tests fail, that's the pre-existing issue, not this task.

- [ ] **Step 6: Commit**

```bash
git add src/tcg_platform/scraping/limitlesstcg.py tests/scraping/test_extract_card_links_from_set_page.py
git commit -m "refactor(limitlesstcg): extract extract_card_links_from_set_page, add P prefix"
```

---

## Task 2: Build pure `build_card_image_diff` helper

**Files:**
- Create: `src/tcg_platform/scraping/limitless_sync.py` (new module for sync-related pure helpers)
- Create: `tests/scraping/test_sync_card_diff.py`

- [ ] **Step 1: Write the failing test**

Create `tests/scraping/test_sync_card_diff.py`:

```python
from tcg_platform.scraping.limitless_sync import (
    build_cdn_url,
    build_minio_key,
    build_card_image_diff,
)


def test_build_cdn_url_base():
    url = build_cdn_url("OP01", "OP01-001", None)
    assert url == "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/OP01/OP01-001_EN.webp"


def test_build_cdn_url_variant():
    url = build_cdn_url("OP01", "OP01-001", 1)
    assert url == "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/OP01/OP01-001_p1_EN.webp"


def test_build_minio_key_base():
    assert build_minio_key("OP01", "OP01-001", None) == "cards/OP01/OP01-001.webp"


def test_build_minio_key_variant():
    assert build_minio_key("OP01", "OP01-001", 1) == "cards/OP01/OP01-001_v1.webp"
    assert build_minio_key("OP01", "OP01-001", 2) == "cards/OP01/OP01-001_v2.webp"


def test_diff_no_new_cards():
    discovered = [
        ("OP01", "OP01-001", None),
        ("OP01", "OP01-002", None),
    ]
    existing = {"cards/OP01/OP01-001.webp", "cards/OP01/OP01-002.webp"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == []


def test_diff_with_new_cards():
    discovered = [
        ("OP01", "OP01-001", None),
        ("OP01", "OP01-002", None),
        ("OP01", "OP01-003", None),
    ]
    existing = {"cards/OP01/OP01-001.webp"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == [
        ("OP01", "OP01-002", None, "cards/OP01/OP01-002.webp"),
        ("OP01", "OP01-003", None, "cards/OP01/OP01-003.webp"),
    ]


def test_diff_includes_variants():
    discovered = [
        ("OP01", "OP01-001", None),
        ("OP01", "OP01-001", 1),
        ("OP01", "OP01-001", 2),
    ]
    existing = {"cards/OP01/OP01-001.webp"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == [
        ("OP01", "OP01-001", 1, "cards/OP01/OP01-001_v1.webp"),
        ("OP01", "OP01-001", 2, "cards/OP01/OP01-001_v2.webp"),
    ]


def test_diff_ignores_legacy_files_in_other_prefixes():
    """Existing files like sold_data/ should not interfere with the cards/ diff."""
    discovered = [("OP01", "OP01-001", None)]
    existing = {"cards/OP01/OP01-001.webp", "sold_data/DE/123.parquet"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/scraping/test_sync_card_diff.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'tcg_platform.scraping.limitless_sync'`

- [ ] **Step 3: Implement the helpers**

Create `src/tcg_platform/scraping/limitless_sync.py`:

```python
"""Pure helpers for syncing Limitless TCG card images to MinIO.

Lives in scraping/ (not defs/) because it has no Dagster dependency and can
be unit-tested in isolation. The Dagster asset wraps these functions.
"""

CDN_BASE = "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece"


def build_cdn_url(set_code: str, card_id: str, variant: int | None) -> str:
    """Build the Limitless CDN URL for a card image.

    Base cards end in _EN.webp; variant N ends in _pN_EN.webp.
    """
    suffix = f"_p{variant}_EN.webp" if variant is not None else "_EN.webp"
    return f"{CDN_BASE}/{set_code}/{card_id}{suffix}"


def build_minio_key(set_code: str, card_id: str, variant: int | None) -> str:
    """Build the MinIO object key for a card image.

    Base cards: cards/{set_code}/{card_id}.webp
    Variant N:  cards/{set_code}/{card_id}_vN.webp
    """
    suffix = f"_v{variant}" if variant is not None else ""
    return f"cards/{set_code}/{card_id}{suffix}.webp"


def build_card_image_diff(
    discovered: list[tuple[str, str, int | None]],
    existing_keys: set[str],
) -> list[tuple[str, str, int | None, str]]:
    """Compute missing images: each entry is (set_code, card_id, variant, minio_key).

    Order of output preserves order of input `discovered` for stable logging.
    """
    out: list[tuple[str, str, int | None, str]] = []
    for set_code, card_id, variant in discovered:
        key = build_minio_key(set_code, card_id, variant)
        if key not in existing_keys:
            out.append((set_code, card_id, variant, key))
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/scraping/test_sync_card_diff.py -v`
Expected: All 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/scraping/limitless_sync.py tests/scraping/test_sync_card_diff.py
git commit -m "feat(limitless_sync): add pure helpers for CDN URL, MinIO key, diff"
```

---

## Task 3: Build `discover_limitless_catalog` Dagster asset

**Files:**
- Create: `src/tcg_platform/defs/discover_limitless_catalog.py`

- [ ] **Step 1: Implement the asset**

Create `src/tcg_platform/defs/discover_limitless_catalog.py`:

```python
import dagster as dg

from tcg_platform.scraping.limitlesstcg import (
    LIMITLESS_OP_BASE,
    _get_all_sets,
    extract_card_links_from_set_page,
)
from playwright.sync_api import sync_playwright


@dg.asset
def discover_limitless_catalog(
    context: dg.AssetExecutionContext,
) -> list[tuple[str, str, int | None]]:
    """Scrape Limitless TCG and return a list of (set_code, card_id, variant).

    Discovers all sets dynamically from /cards/ (not hardcoded). For each set,
    scrapes the set page and extracts base cards + ?v=N variant links. Output
    is a flat list of tuples; no MinIO writes.
    """
    catalog: list[tuple[str, str, int | None]] = []
    seen: set[tuple[str, str, int | None]] = set()

    sets = _get_all_sets()
    context.log.info(f"Discovered {len(sets)} sets on Limitless")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for set_code, set_path in sets:
            try:
                page = browser.new_page()
                page.goto(f"{LIMITLESS_OP_BASE}{set_path}", timeout=60000)
                page.wait_for_load_state("networkidle", timeout=30000)
                html = page.content()
                page.close()
            except Exception as e:
                context.log.warning(f"Failed to scrape set {set_code} ({set_path}): {e}; skipping")
                continue

            for card_id, variant in extract_card_links_from_set_page(html):
                key = (set_code, card_id, variant)
                if key not in seen:
                    seen.add(key)
                    catalog.append(key)

        browser.close()

    if not catalog:
        raise RuntimeError(
            "Discovery returned 0 cards. Limitless HTML may have changed; "
            "check extract_card_links_from_set_page and _get_all_sets in "
            "src/tcg_platform/scraping/limitlesstcg.py"
        )

    context.log.info(f"Discovery complete: {len(catalog)} (set, card, variant) tuples across {len(sets)} sets")
    return catalog
```

- [ ] **Step 2: Verify the asset loads via Dagster's auto-discovery**

Run: `.venv/bin/python -c "from tcg_platform.definitions import defs; resolved = defs.load_fn(); assets = list(resolved.assets); names = [a.key.to_user_string() for a in assets]; print('discover_limitless_catalog' in names); print([n for n in names if 'limitless' in n.lower() or 'sync_card' in n.lower()])"`

Expected: prints `True` and a list containing `discover_limitless_catalog` (and the new `sync_card_images` once Task 4 lands).

- [ ] **Step 3: Commit**

```bash
git add src/tcg_platform/defs/discover_limitless_catalog.py
git commit -m "feat(defs): add discover_limitless_catalog asset"
```

---

## Task 4: Build `sync_card_images` Dagster asset

**Files:**
- Create: `src/tcg_platform/defs/sync_card_images.py`

- [ ] **Step 1: Implement the asset**

Create `src/tcg_platform/defs/sync_card_images.py`:

```python
import time
import dagster as dg
import requests
from dagster import AssetIn

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.scraping.limitless_sync import (
    build_cdn_url,
    build_card_image_diff,
)


@dg.asset(
    required_resource_keys={"minio_client"},
    ins={"discover_limitless_catalog": AssetIn()},
)
def sync_card_images(
    context: dg.AssetExecutionContext,
    discover_limitless_catalog: list[tuple[str, str, int | None]],
) -> dg.MaterializeResult:
    """Diff the discovered Limitless catalog against tcg-bronze/cards/, download missing."""
    minio_client: MinioClientResource = context.resources.minio_client
    bucket = minio_client.bucket_name

    started = time.time()
    existing_keys = set(minio_client.list_objects(bucket, "cards/"))
    context.log.info(f"MinIO has {len(existing_keys)} existing objects under cards/")

    diff = build_card_image_diff(discover_limitless_catalog, existing_keys)
    context.log.info(f"Diff: {len(diff)} new images to download (catalog size: {len(discover_limitless_catalog)})")

    new_card_ids: list[str] = []
    failed_card_ids: list[str] = []

    for set_code, card_id, variant, key in diff:
        url = build_cdn_url(set_code, card_id, variant)
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.content
        except Exception as e:
            context.log.warning(f"CDN fetch failed for {key} ({url}): {e}")
            failed_card_ids.append(card_id)
            continue

        try:
            minio_client.put_object(
                bucket_name=bucket,
                object_name=key,
                data=data,
                length=len(data),
                content_type="image/webp",
            )
            new_card_ids.append(card_id)
        except Exception as e:
            context.log.warning(f"put_object failed for {key}: {e}")
            failed_card_ids.append(card_id)

    duration = round(time.time() - started, 2)
    context.log.info(
        f"Sync complete in {duration}s: "
        f"discovered={len(discover_limitless_catalog)} existing={len(existing_keys)} "
        f"new={len(new_card_ids)} failed={len(failed_card_ids)}"
    )

    return dg.MaterializeResult(
        metadata={
            "discovered_count": len(discover_limitless_catalog),
            "existing_count": len(existing_keys),
            "new_count": len(new_card_ids),
            "failed_count": len(failed_card_ids),
            "new_card_ids": dg.MetadataValue.json(new_card_ids),
            "failed_card_ids": dg.MetadataValue.json(failed_card_ids),
            "duration_seconds": duration,
        }
    )
```

- [ ] **Step 2: Verify the asset loads via Dagster's auto-discovery**

Run: `.venv/bin/python -c "from tcg_platform.definitions import defs; resolved = defs.load_fn(); assets = list(resolved.assets); names = [a.key.to_user_string() for a in assets]; print('sync_card_images' in names and 'discover_limitless_catalog' in names)"`

Expected: prints `True`.

- [ ] **Step 3: Commit**

```bash
git add src/tcg_platform/defs/sync_card_images.py
git commit -m "feat(defs): add sync_card_images asset (diff + CDN download)"
```

---

## Task 5: Register `sync_card_images_job` in `definitions.py`

**Files:**
- Modify: `src/tcg_platform/definitions.py`

- [ ] **Step 1: Add the job definition**

In `src/tcg_platform/definitions.py`, add `sync_card_images_job` after the existing `complete_eu_pipeline` definition (after line 72). Then add it to the `jobs=[...]` list (currently lines 81-91).

Insert after the existing `complete_eu_pipeline = define_asset_job(...)` block:

```python
sync_card_images_job = define_asset_job(
    name="sync_card_images_job",
    selection=["discover_limitless_catalog", "sync_card_images"],
    description="Diff Limitless catalog against tcg-bronze/cards/, download missing images.",
)
```

Then add `sync_card_images_job` to the `jobs=[...]` list in the same order (after `complete_eu_pipeline`):

```python
        jobs=[
            ebay_de_job,
            ebay_uk_job,
            ebay_eu_job,
            backfill_de_job,
            backfill_uk_job,
            silver_de_job,
            silver_uk_job,
            silver_eu_job,
            complete_eu_pipeline,
            sync_card_images_job,
        ],
```

- [ ] **Step 2: Verify Dagster definitions load cleanly**

Run: `.venv/bin/python -c "from tcg_platform.definitions import defs; print('OK')"`

Expected: prints `OK`. (No exceptions, no warnings about missing assets.)

- [ ] **Step 3: Verify the job is registered and references the right assets**

Run: `.venv/bin/python -c "
from tcg_platform.definitions import defs
resolved = defs.load_fn()
job = resolved.resolve_job_def('sync_card_images_job')
print('Job name:', job.name)
print('Asset keys:', [k.to_user_string() for k in job.asset_layer.asset_keys_for_node('sync_card_images_job_op') if False] or [k.to_user_string() for k in job.asset_layer.asset_keys])
"

Expected: prints job name `sync_card_images_job` and asset keys including `discover_limitless_catalog` and `sync_card_images`.

(Note: if the exact attribute path for asset keys on the JobDefinition differs, the assertion in step 2 that `print('OK')` succeeded is the load-clean check. Step 3 is best-effort confirmation; if it errors, fall back to step 2 only.)

- [ ] **Step 4: Commit**

```bash
git add src/tcg_platform/definitions.py
git commit -m "feat(definitions): register sync_card_images_job alongside complete_eu_pipeline"
```

---

## Task 6: End-to-end verification

- [ ] **Step 1: Run the full test suite**

Run: `.venv/bin/pytest tests/ -v`

Expected: 70+ tests collected, 68+ passing. The 2 pre-existing `test_exchange_rate.py` failures are unrelated. New tests in `test_extract_card_links_from_set_page.py` (6 tests) and `test_sync_card_diff.py` (8 tests) all pass.

- [ ] **Step 2: Verify Dagster definitions load cleanly (final)**

Run: `.venv/bin/python -c "from tcg_platform.definitions import defs; print('OK')"`

Expected: `OK`

- [ ] **Step 3: Verify working tree is clean of unplanned changes**

Run: `git status --porcelain`

Expected: empty output, or only the files added in Tasks 1-5 plus the SESSION log about to be added.

- [ ] **Step 4: Write SESSION_2026-06-07.md**

Create `log/SESSION_2026-06-07.md`:

```markdown
# Session Log — 2026-06-07

## Branch
`2026-06-07-card-image-sync`

## What was done

Built the M8-T1 Limitless card image sync job: a single-button Dagster
pipeline that keeps `tcg-bronze/cards/` in sync with the current Limitless
TCG card catalog. New sets and new cards (base + `?v=N` variants) get picked
up automatically; existing files are skipped via a single `list_objects`
diff.

### Refactor — `extract_card_links_from_set_page`
Extracted the inline set-page link-parsing logic from
`scrape_limitless_op` into a module-level pure function. Added `P` (promo
cards) to the prefix set — the original filter missed them but the M2-T3
era MinIO data includes a `cards/P/` set. The `scrape_limitless_op` call
sites now use the helper; behavior is otherwise unchanged.

### New helpers — `src/tcg_platform/scraping/limitless_sync.py`
Three pure functions: `build_cdn_url` (Limitless CDN URL with `_pN_EN.webp`
suffix for variants), `build_minio_key` (`cards/{set}/{card}_vN.webp` format),
and `build_card_image_diff` (in-memory diff against an existing-keys set).
All 8 helper tests pass; the diff is exact-match on the MinIO key, not a
fuzzy card_id match.

### New assets
- `discover_limitless_catalog` (Dagster) — Playwright scrapes `/cards/` and
  each set page, returns `[(set_code, card_id, variant), ...]`. Dynamic;
  no hardcoded set list. Fails loud if the result is empty (HTML drift).
- `sync_card_images` (Dagster) — `list_objects("tcg-bronze", "cards/")`,
  diff, CDN-fetch missing, `put_object` with `image/webp` content type.
  Per-card try/except: failures go into `failed_card_ids`, don't abort the
  job. Returns MaterializeResult with counts and new/failed card_id lists.

### New job
`sync_card_images_job` registered in `definitions.py` alongside
`complete_eu_pipeline`. Dagster UI: one button, same shape as the EU
pipeline orchestrator.

## Test counts (end of session)

70+ tests collected. All pre-existing tests still pass; 14 new tests
added across `test_extract_card_links_from_set_page.py` (6) and
`test_sync_card_diff.py` (8). The 2 pre-existing
`test_exchange_rate.py` failures remain out of scope.

## Commits
```
91396f1  docs: add spec for Limitless card image sync job (M8-T1 design)
<refactor commit>  refactor(limitlesstcg): extract extract_card_links_from_set_page, add P prefix
<sync helpers commit>  feat(limitless_sync): add pure helpers for CDN URL, MinIO key, diff
<discover commit>  feat(defs): add discover_limitless_catalog asset
<sync asset commit>  feat(defs): add sync_card_images asset (diff + CDN download)
<job commit>  feat(definitions): register sync_card_images_job alongside complete_eu_pipeline
```

## What remains
- Manual run: launch `sync_card_images_job` from the Dagster UI on a
  monthly cadence (or whenever Limitless publishes a new set).
- The `bronze/cardlist/...` writer and the silver `is_valid_card_id` path
  bug (separate tasks).
- The 2 pre-existing `test_exchange_rate.py` failures (separate task).
```

- [ ] **Step 5: Commit the session log**

```bash
git add log/SESSION_2026-06-07.md
git commit -m "docs: session log for 2026-06-07 (M8-T1 card image sync)"
```

- [ ] **Step 6: Push branch to remote**

Per AGENTS.md Rule 18:

```bash
git push origin 2026-06-07-card-image-sync
```

Expected: branch pushed successfully. (Merge to main is human-driven per Rule 19.)

---

## Self-Review

**Spec coverage check:**

| Spec section | Task |
|---|---|
| Goal (single-button sync job) | Tasks 3-5 |
| Why now (replace deleted M2-T3) | Context, no task needed |
| Scope (images only, dynamic sets, idempotent) | Tasks 3-4 |
| Out of scope (cardlist parquet, scheduling, etc.) | Stated in SESSION log |
| Architecture (2 assets, 1 job) | Tasks 3-5 |
| Asset 1: `discover_limitless_catalog` (Playwright, returns tuples) | Task 3 |
| Asset 2: `sync_card_images` (list_objects, diff, CDN fetch, put_object) | Task 4 |
| Job: `sync_card_images_job` (define_asset_job, selection, register) | Task 5 |
| Data flow diagram | Implicit in tasks; no new task needed |
| Refactor: extract `extract_card_links_from_set_page`, add P prefix | Task 1 |
| Error handling (per-card try/except, fail-loud on empty discovery) | Tasks 3, 4 |
| Tests (5 cases for parser, 7 cases for diff helpers) | Tasks 1, 2 |
| Files added / modified | Tasks 1-5 |
| Verification (pytest, definitions load, status clean) | Task 6 |
| Out-of-scope follow-ups | SESSION log + spec |

**Placeholder scan:** No "TBD", "TODO", "fill in later", or vague steps. All code blocks contain real code.

**Type consistency:** `tuple[str, str, int | None]` is used consistently across the spec, helpers, tests, and asset signatures. `build_minio_key` and `build_cdn_url` signatures match between tests and implementation. The asset's `discover_limitless_catalog` return type matches the `sync_card_images` `ins={...}` input.

**One minor note:** Task 5 Step 3's exact `JobDefinition` attribute path for asset keys is best-effort; the load-clean check in Step 2 is the authoritative verification. This is flagged in the step itself.
