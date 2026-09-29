# Ekart Dispatch Report → BigQuery (one file)

`main.py` is the **only** file you need. One command replaces the entire manual
routine (~1–1.2 hrs daily → ~4 min, unattended):

```
Eduvate login → latest Dispatch Report run → download ALL parts (N-part agnostic)
→ consolidate → keep the 13 BigQuery columns → clean
→ upload to inventory-automation-2026.inventory_data.orders_data (replace)
→ verify row count in BigQuery → clean exit
```

## Usage

```bash
python main.py               # FULL pipeline: download + consolidate + upload
python main.py --list        # show the latest report runs, then exit
python main.py --run-id 738  # process a specific historical run
python main.py --no-upload   # download + consolidate only (dry run)
python main.py --keep-xlsx   # keep downloaded part files after finishing
```

## Files in this folder

| File | Purpose |
|---|---|
| `main.py` | the whole automation (single file) |
| `config.json` | Eduvate credentials + BigQuery settings (**never commit**) |
| `downloads/`, `output/` | working data (git-ignored) |

## Requirements

```
pip install requests pandas openpyxl python-calamine pyarrow pandas-gbq db-dtypes
```

## Config (`config.json`)

```json
{
  "username": "<ERP_ID>",
  "password": "<password>",
  "finance_session_year": 47,
  "gcp": {
    "project_id": "inventory-automation-2026",
    "dataset_id": "inventory_data",
    "table_name": "orders_data",
    "key_file": "D:\\Work Related\\Automation\\inventory-automation-2026\\key.json"
  }
}
```

`key_file` points to your existing service-account key (same one the manual
script used). If the key moves, update only this path.

## Column mapping (Excel → BigQuery)

Branch Name→Branch_Name · Grade Name→Grade · ERP ID→ERP_ID ·
Ekart Order No→Ekart_Order_No · Payment Date→Payment_Date · Item SKU→SKU ·
Item Name→Item_Name · Quantity→Quantity · Order Type→Order_Type ·
Current Status→Status · Volume→Volume · Ekart Order Created At→Created_Date ·
Transaction No→Transaction_No

Cleaning: `Payment_Date`/`Created_Date` → datetime (invalid → NULL),
`Quantity` → int32, `Transaction_No`/`SKU` → Int64 (blanks stay NULL).

## How "latest report" is guaranteed

The script never trusts API ordering. It picks the run by **data**: only runs
with `status == success` **and** non-empty `file_urls` are candidates, then
`max(created_at)` wins (ties → higher id). So an in-progress run that appears
on top of the portal is explicitly skipped with a NOTE, and a stale report
(>12h / >24h old) triggers a loud warning.

Before any upload there is a **row-count safety stop**: consolidated rows must
equal the run's `total_records` (checked live on 2026-09-24: 1,036,592 =
1,036,592). A mismatch blocks the upload unless you pass
`--allow-count-mismatch`.

## Notes

- Upload mode is `replace` (table is fully rewritten each run, same as the
  manual flow). Change via `"bq_if_exists": "append"` in config.json if needed.
- The process force-exits after finishing — intentional: pandas-gbq leaves
  background threads hanging on Windows otherwise.
- Downloads are cached per run id (`downloads/run_<id>/`), so re-runs resume
  instead of re-downloading.
- Session year changes each April (e.g. 47 → 48): update `finance_session_year`.

## Verified

2026-09-24: full pipeline ran live — 10,36,592 rows downloaded from run 738,
consolidated (3 parts), cleaned, uploaded to `orders_data`; BigQuery
`COUNT(*)` confirmed 1,036,592 rows.
