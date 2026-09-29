"""
main.py — Ekart Dispatch Report -> BigQuery (COMPLETE DAILY AUTOMATION)
=======================================================================
One file. One command. Replaces the entire manual flow:

    Eduvate login -> latest Dispatch Report run -> download ALL parts
    -> consolidate -> keep only the 13 BigQuery columns -> clean
    -> upload to inventory-automation-2026.inventory_data.orders_data
    -> verify row count -> exit cleanly

Usage:
    python main.py               # FULL pipeline (download + consolidate + upload)
    python main.py --list        # show the latest report runs, then exit
    python main.py --run-id 738  # process a specific historical run
    python main.py --no-upload   # download + consolidate only (dry run)
    python main.py --keep-xlsx   # keep downloaded part files after finishing

Credentials & BigQuery settings live in config.json (same folder).
Requires: requests pandas openpyxl python-calamine pyarrow pandas-gbq db-dtypes
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
import numpy as np
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

# ============================================================
# 1. CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"

LOGIN_URL = "https://orchids.letseduvate.com/qbox/erp_user/user-mgmt/staff-login/"
REPORT_URL = ("https://orchids.finance.letseduvate.com/qbox/ekart/"
              "branch-wise-dispatch-report/")

CHUNK = 1 << 20  # 1 MiB

# Excel column name -> BigQuery/DataFrame column name
EXPECTED_COLUMNS = [
    "Zone ID",
    "Zone Name",
    "Branch ID",
    "Branch Name",
    "Grade Name",
    "Branch Pin Code",
    "City",
    "Student Name",
    "ERP ID",
    "Ekart Order No",
    "Ekart Tracking No",
    "Ekart Order Created At",
    "Transaction No",
    "Payment Date",
    "Payment Month",
    "Item SKU",
    "Item Name",
    "Quantity",
    "Docket ID",
    "Invoice ID",
    "Sub Category Name",
    "Volume",
    "Order Type",
    "Expected Delivery Date",
    "Packed DateTime",
    "Shipped DateTime",
    "Delivery DateTime",
    "Recieved By Parent DateTime",
    "Current Status",
    "Sales Order",
    "Is Small Packet",
    "Ekart OTP",
]

COLS_MAP = {
    "Zone ID": "zone_id",
    "Zone Name": "zone_name",
    "Branch ID": "branch_id",
    "Branch Name": "branch_name",
    "Grade Name": "grade_name",
    "Branch Pin Code": "branch_pin_code",
    "City": "city",
    "Student Name": "student_name",
    "ERP ID": "erp_id",
    "Ekart Order No": "ekart_order_no",
    "Ekart Tracking No": "ekart_tracking_no",
    "Ekart Order Created At": "ekart_order_created_at",
    "Transaction No": "transaction_no",
    "Payment Date": "payment_date",
    "Payment Month": "payment_month",
    "Item SKU": "item_sku",
    "Item Name": "item_name",
    "Quantity": "quantity",
    "Docket ID": "docket_id",
    "Invoice ID": "invoice_id",
    "Sub Category Name": "sub_category_name",
    "Volume": "volume",
    "Order Type": "order_type",
    "Expected Delivery Date": "expected_delivery_date",
    "Packed DateTime": "packed_datetime",
    "Shipped DateTime": "shipped_datetime",
    "Delivery DateTime": "delivery_datetime",
    "Recieved By Parent DateTime": "recieved_by_parent_datetime",
    "Current Status": "current_status",
    "Sales Order": "sales_order",
    "Is Small Packet": "is_small_packet",
    "Ekart OTP": "ekart_otp",
}


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        sys.exit(f"ERROR: config not found at {CONFIG_PATH}")
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    for key in ("username", "password"):
        if not cfg.get(key):
            sys.exit(f"ERROR: '{key}' missing in config.json")
    gcp = cfg.get("gcp")
    if not gcp or not all(gcp.get(k) for k in ("project_id", "dataset_id", "table_name", "key_file")):
        sys.exit("ERROR: gcp {project_id, dataset_id, table_name, key_file} missing in config.json")
    return cfg


# ============================================================
# 2. EDUVATE API: LOGIN + LATEST RUN + DOWNLOAD
# ============================================================

def eduvate_login(cfg: dict) -> requests.Session:
    log("Logging in to Eduvate ...")
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    r = s.post(LOGIN_URL, json={
        "username": cfg["username"],
        "password": cfg["password"],
        "unified_login": True,
    }, timeout=60)
    r.raise_for_status()
    body = r.json()
    if body.get("status_code") != 200:
        sys.exit(f"Login failed: {body.get('message')}")
    s.headers["Authorization"] = f"Bearer {body['result']['access']}"
    log("Login OK")
    return s


def fetch_reports(session: requests.Session, session_year: int, page: int = 1) -> list:
    r = session.get(f"{REPORT_URL}?finance_session_year={session_year}&page={page}", timeout=120)
    r.raise_for_status()
    body = r.json()
    if body.get("status_code") != 200:
        sys.exit(f"Report list failed: {body.get('message')}")
    return body["result"]["results"]


def download_part(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        log(f"  {dest.name} already downloaded - skipping (resume)")
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    t0 = time.time()
    with requests.get(url, stream=True, timeout=(30, 900)) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=CHUNK):
                fh.write(chunk)
    tmp.replace(dest)  # Path.replace overwrites on Windows too
    log(f"  {dest.name} downloaded ({dest.stat().st_size/1048576:,.1f} MB "
        f"in {time.time()-t0:,.0f}s)")
    return dest


def parse_created_at(run: dict) -> datetime:
    """Parse run['created_at'] (ISO like 2026-09-24T02:40:03) safely."""
    ts = run.get("created_at") or ""
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return datetime.min  # unknown timestamps sort oldest


def get_latest_run_parts(session: requests.Session, session_year: int,
                         run_id: int | None,
                         now: datetime | None = None) -> tuple[dict, list[Path]]:
    """Always pick the LATEST COMPLETED run - guaranteed by data, not API order.

    Guarantees:
      * candidates = runs with status == 'success' AND non-empty file_urls
      * pick max(created_at) among candidates (ties -> higher id)
      * never picks a run that is still generating / failed / has no files
      * warns loudly when skipping newer not-yet-ready runs or when the
        chosen run is stale (>12h old)
    """
    reports = fetch_reports(session, session_year)
    now = now or datetime.now()

    if run_id:
        run = next((r for r in reports if r["id"] == run_id), None)
        if run is None:
            sys.exit(f"Run id {run_id} not found on page 1 (use --list).")
    else:
        candidates = [r for r in reports
                      if r.get("status") == "success" and (r.get("file_urls") or [])]
        if not candidates:
            sys.exit("No completed Dispatch Report runs found. The report may still be "
                     "generating - try again later.")

        run = max(candidates, key=lambda r: (parse_created_at(r), r["id"]))

        # transparency: anything NEWER that we deliberately skipped?
        newer = [r for r in reports if r is not run
                 and parse_created_at(r) > parse_created_at(run)]
        for r in newer[:3]:
            log(f"NOTE: skipping run {r['id']} (created "
                f"{parse_created_at(r):%d %b %H:%M}, status={r.get('status')}) - "
                f"not completed / no files yet")

        # staleness guard: report should exist ~2x daily (02:40 & 13:00)
        age_h = (now - parse_created_at(run)).total_seconds() / 3600
        if age_h > 24:
            log(f"WARNING: latest completed run is {age_h:.0f}h old "
                f"({run.get('created_at', '')[:16]}). No fresh report today? "
                "Uploading it anyway - verify this is intended.")
        elif age_h > 12:
            log(f"WARNING: latest completed run is {age_h:.1f}h old - "
                "today's newer run may not be generated yet.")

    if run.get("status") != "success":
        sys.exit(f"Run {run['id']} status='{run.get('status')}' - not ready. "
                 f"Try again later or pick another run.")
    urls = run.get("file_urls") or []
    if not urls:
        sys.exit(f"Run {run['id']} has no file_urls - nothing to download.")

    log(f"Run {run['id']} | {run.get('created_at', '')[:16]} | "
        f"{run.get('total_records'):,} records | {len(urls)} part(s)")

    dl_dir = ROOT / "downloads" / f"run_{run['id']}"
    paths = []
    for i, u in enumerate(urls, 1):
        ext = ".xlsx" if ".xlsx" in u.split("?")[0] else ".bin"
        paths.append(download_part(u, dl_dir / f"part{i}{ext}"))
    return run, paths


# ============================================================
# 3. PROCESS ONE FILE (parallel worker)
# ============================================================

def process_single_file(path: Path) -> dict:
    """Read one xlsx, validate columns, select + rename for BigQuery."""
    log(f"  reading {path.name} ...")
    try:
        df = pd.read_excel(path, engine="calamine")  # Rust engine: 10-50x faster than openpyxl
    except Exception as exc:
        log(f"  FAILED reading {path.name}: {exc}")
        return {"file": path.name, "rows": 0, "data": None, "error": str(exc)}

    missing = set(COLS_MAP) - set(df.columns)
    if missing:
        log(f"  REJECTED {path.name} - missing columns: {sorted(missing)}")
        return {"file": path.name, "rows": 0, "data": None,
                "error": f"Missing required columns: {sorted(missing)}"}

    df = df[list(COLS_MAP)].rename(columns=COLS_MAP)
    log(f"  {path.name}: {len(df):,} rows x {len(df.columns)} cols -> OK")
    return {"file": path.name, "rows": len(df), "data": df, "error": None}


# ============================================================
# 4. CONSOLIDATE + CLEAN
# ============================================================

def consolidate(paths: list[Path]) -> pd.DataFrame | None:
    log(f"Processing {len(paths)} file(s) in parallel ...")
    results = []
    with ThreadPoolExecutor(max_workers=min(len(paths), 4)) as ex:
        futures = [ex.submit(process_single_file, p) for p in paths]
        for f in as_completed(futures):
            results.append(f.result())

    failed = [r for r in results if r["error"]]
    if failed:
        for r in failed:
            log(f"FILE FAILED: {r['file']} -> {r['error']}")
        log("PIPELINE STOPPED - BigQuery will NOT be updated.")
        return None
    if len(results) != len(paths):
        log("PIPELINE STOPPED - not all files returned.")
        return None

    parts_rows = sum(r["rows"] for r in results)
    final_df = pd.concat([r["data"] for r in results], ignore_index=True)

    if len(final_df) != parts_rows:
        log(f"ROW MISMATCH! expected {parts_rows:,} got {len(final_df):,} - stopping.")
        return None

    log(f"Consolidated: {len(final_df):,} rows x {len(final_df.columns)} cols "
        f"(validation passed)")

    # ---- cleaning ----
    for col in ("Payment_Date", "Created_Date"):
        if col in final_df.columns:
            final_df[col] = pd.to_datetime(final_df[col], errors="coerce")
            log(f"  {col}: {final_df[col].isna().sum():,} blank/invalid dates")

    if "Quantity" in final_df.columns:
        final_df["Quantity"] = pd.to_numeric(final_df["Quantity"], errors="coerce") \
            .fillna(0).astype("int32")
        log("  Quantity -> int32")

    # keep large ids as integers (not floats) so BigQuery schema stays stable
    for col in ("Transaction_No", "SKU"):
        if col in final_df.columns:
            raw = pd.to_numeric(final_df[col], errors="coerce").to_numpy(dtype="float64")
            mask = np.isnan(raw)
            vals = np.nan_to_num(raw, nan=0.0, posinf=0.0, neginf=0.0)
            ints = np.rint(vals).astype("int64")
            final_df[col] = pd.arrays.IntegerArray(ints, mask)
            log(f"  {col} -> Int64 ({int(mask.sum()):,} blanks)")

    log(f"Final dataset: {len(final_df):,} rows x {len(final_df.columns)} cols")
    return final_df


# ============================================================
# 5. UPLOAD TO BIGQUERY
# ============================================================

def upload_to_bigquery(final_df: pd.DataFrame, cfg: dict) -> bool:
    gcp = cfg["gcp"]
    key_path = Path(gcp["key_file"])
    if not key_path.exists():
        log(f"BigQuery key file not found: {key_path}")
        log("PIPELINE STOPPED - BigQuery will NOT be updated.")
        return False

    table = f"{gcp['dataset_id']}.{gcp['table_name']}"
    log(f"Uploading {len(final_df):,} rows -> {gcp['project_id']}.{table}")
    t0 = time.time()
    try:
        credentials = service_account.Credentials.from_service_account_file(str(key_path))
        pandas_gbq.to_gbq(
            final_df,
            table,
            project_id=gcp["project_id"],
            credentials=credentials,
            if_exists=cfg.get("bq_if_exists", "replace"),
        )
    except Exception as exc:
        log(f"BIGQUERY UPLOAD FAILED: {exc}")
        return False

    log(f"Upload done in {time.time()-t0:,.0f}s - verifying row count in BigQuery ...")
    try:
        count = pandas_gbq.read_gbq(
            f"SELECT COUNT(*) AS n FROM `{gcp['project_id']}.{table}`",
            project_id=gcp["project_id"],
            credentials=credentials,
        )["n"].iloc[0]
        log(f"BigQuery verification: {count:,} rows in {table}")
    except Exception as exc:
        log(f"(Verification query failed: {exc})")

    log("BIGQUERY UPLOAD SUCCESSFUL")
    return True


# ============================================================
# 6. MAIN
# ============================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Ekart Dispatch -> BigQuery daily automation")
    ap.add_argument("--list", action="store_true", help="list latest report runs and exit")
    ap.add_argument("--run-id", type=int, help="use a specific run id instead of the latest")
    ap.add_argument("--no-upload", action="store_true", help="download + consolidate only")
    ap.add_argument("--keep-xlsx", action="store_true", help="keep downloaded part files")
    ap.add_argument("--allow-count-mismatch", action="store_true",
                    help="upload even if consolidated rows != run's total_records")
    args = ap.parse_args()

    cfg = load_config()

    if args.list:
        session = eduvate_login(cfg)
        for r in fetch_reports(session, int(cfg.get("finance_session_year", 47)))[:10]:
            rec = r.get("total_records")
            rec_s = f"{int(rec):>9,}" if rec is not None else "    (running)"
            log(f"id={r['id']:<5} {r.get('created_at', '')[:16]}  "
                f"status={r.get('status') or '?':<12} records={rec_s}  "
                f"parts={len(r.get('file_urls') or [])}")
        return

    t_start = time.time()
    session = eduvate_login(cfg)
    run, paths = get_latest_run_parts(session, int(cfg.get("finance_session_year", 47)),
                                      args.run_id)
    final_df = consolidate(paths)
    if final_df is None or final_df.empty:
        sys.exit("PIPELINE TERMINATED - no data to upload.")

    # data-safety stop: consolidated rows must match the run's reported total.
    # 20 lakh rows landed in BigQuery with a wrong count = bad day.
    reported = run.get("total_records")
    if (reported and len(final_df) != int(reported)
            and not args.allow_count_mismatch):
        log(f"ROW COUNT MISMATCH vs report metadata: consolidated "
            f"{len(final_df):,} but run reports {int(reported):,} "
            f"(diff {len(final_df) - int(reported):+,})")
        log("Upload BLOCKED. Check the report on the portal, or upload anyway "
            "with --allow-count-mismatch.")
        sys.exit(1)

    if args.no_upload:
        stamp = datetime.now().strftime("%Y%m%d")
        out = ROOT / cfg.get("output_dir", "output")
        out.mkdir(parents=True, exist_ok=True)
        csv_gz = out / f"ekart_bq_ready_{stamp}.csv.gz"
        final_df.to_csv(csv_gz, index=False, compression={"method": "gzip", "compresslevel": 5})
        log(f"--no-upload: saved BQ-ready dataset to {csv_gz}")
        return

    ok = upload_to_bigquery(final_df, cfg)

    if not args.keep_xlsx:
        for p in paths:
            p.unlink(missing_ok=True)
        log("Removed downloaded part files (use --keep-xlsx to keep them)")

    print("\n" + "=" * 60)
    if ok:
        log(f"PIPELINE COMPLETED: {len(final_df):,} rows uploaded in "
            f"{time.time()-t_start:,.0f}s total")
        exit_code = 0
    else:
        log("PIPELINE FAILED - see errors above.")
        exit_code = 1

    # pandas-gbq leaves background gRPC threads hanging on Windows after the
    # work is done; flush output and force-exit so runs terminate cleanly.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(exit_code)


if __name__ == "__main__":
    main()
