# Retail Data Warehouse — Architecture Notes

## 1. Architecture

**Source files → Bronze → Silver → Gold → Marts / consumers**

The Snowflake environment uses warehouse `RETAIL_WH`, database `RETAIL_DW`, and schemas `BRONZE`, `SILVER`, `GOLD`, and `MARTS`.

This document focuses on the layer contracts, quality gates, source-to-layer mapping, and reload policy required for Assignment 2.2. It distinguishes current implementation from proposed controls.

## 2. Layer contracts

| Layer | Contract | Current implementation / limitation |
|---|---|---|
| **Bronze** | Preserve source data as received, with source filename, row number, and ingestion timestamp wherever available. Do not apply business corrections here. Keep data replayable. | `RAW_SOURCE_ROWS` stores generic `C1`–`C30` VARCHAR fields and lineage columns. Dedicated raw RMS tables also exist. The stage contains the source files. The submitted Bronze load script does not show COPY statements for all 64 files. |
| **Silver** | Parse and standardize source data; retain source lineage; validate required fields; isolate invalid records in quarantine rather than silently dropping them. | Inventory parsing and quarantine are documented. Other Silver tables and their row counts were verified, but their transformation SQL is not included in the submitted scripts. The current inventory SQL does not quarantine every invalid value (for example, unparseable dates and non-numeric quantities). |
| **Gold** | Publish business-ready dimensions and facts at their defined grain, with valid keys and consistent measures. Apply dimension history rules where required. | All ten Assignment 2.1 objects were created. Most are full rebuilds using `CREATE OR REPLACE`. `DIM_ITEM` and `DIM_LOCN` do not implement the requested SCD Type 2 history in the submitted SQL. `DIM_CUSTOMER` is empty. |
| **Marts / consumers** | Provide purpose-built datasets for reporting and analytics, sourced from validated Gold data. | The `MARTS` schema exists, but no mart tables or BI connections are created in the submitted scripts. |

## 3. Source-to-layer mapping

All source files are staged in Bronze. The following shows the main Silver objects and their current direct use in Gold.

| Source family | Silver representation | Current Gold use |
|---|---|---|
| RMS item master | `RMS_ITEM_MASTER` | `DIM_ITEM` |
| RMS subclasses | `RMS_SUBCLASSES` | `DIM_SUBCLASS` |
| RMS districts | `RMS_DISTRICTS` | `DIM_DISTRICT` |
| RMS stores | `RMS_STORES` | `DIM_LOCN` |
| Finance fiscal calendar | `FISCAL_CALENDAR` | `DIM_DAY` |
| RMS SOH snapshots | `INVENTORY_SNAPSHOT` | `FCT_INVENTORY` |
| Xstore sales | `XSTORE_SALES_RECOVERED` | `FCT_SALES`, `AGG_DAILY_ITEM_SALES` |
| RPM price events | `PRICE_EVENTS` | `FCT_PRICE` |
| RMS UPC | `RMS_ITEM_UPC` | Supporting recovery of missing Xstore item IDs; no direct Gold dimension load shown |
| RMS transactions and item history | `RMS_TRANSACTIONS`, `RMS_ITEM_HISTORY` | No direct Gold load shown |
| NCR sales | `NCR_SALES` | No direct Gold sales load shown |
| OMS web orders | `WEB_ORDER_LINES` | No direct Gold sales load shown |
| PIM style export | `PIM_STYLE_EXPORT` | No direct Gold load shown |
| RPAS forecast | `DEMAND_FORECAST` | No direct Gold load shown |
| WMS movements and snapshots | `DC_MOVEMENTS`, `DC_ONHAND_SNAPSHOT` | No direct Gold load shown |
| Other RMS reference data and store roster | Corresponding Silver reference tables | No direct Gold load shown |

A source being present in Bronze or Silver does not mean it is currently included in Gold.

## 4. Quality gates

The gates below define the proposed acceptance criteria at each layer boundary. The listed actions are **proposed controls**, not automated behavior in the submitted SQL.

| Boundary | Gate / rule | Failure action |
|---|---|---|
| Bronze → Silver | Snapshot date must parse and fall within the accepted date range. | `QUARANTINE` |
| Bronze → Silver | Inventory quantity must be numeric. | `QUARANTINE` |
| Bronze → Silver | Required source keys (such as item and location identifiers) must be present. | `QUARANTINE` |
| Silver → Gold | Sales item IDs must resolve to `DIM_ITEM`. | `QUARANTINE` |
| Silver → Gold | Sales dates must resolve to `DIM_DAY`. | `FAIL` |
| Silver → Gold | Sales locations must resolve to `DIM_LOCN`. | `FAIL` |
| Silver → Gold | Item subclass references must resolve to `DIM_SUBCLASS`. | `ALERT` |
| Silver → Gold | Price effective start date must not be after effective end date. | `QUARANTINE` |
| Silver → Gold | Overlapping price events for the same business key must be detected. | `ALERT` |
| Gold validation | Required row-count and key-relationship checks must pass after a rebuild. | `FAIL` |

**Action definitions**
- `FAIL`: stop the affected load or publication.
- `QUARANTINE`: isolate the rejected record, retain its reason and lineage, and prevent it from entering the next layer.
- `ALERT`: report the exception for investigation; do not silently ignore it.

### Existing validation evidence

Recorded exceptions were:
- 1,785 sales rows have item IDs `0` or `99999999` that do not match `DIM_ITEM`. These rows are present in `FCT_SALES`.
- 23 items have unresolved subclass references.
- Location `503` references district `114`, which is absent from `DIM_DISTRICT`.
- Sales-to-location and sales-to-date checks returned zero missing references after the `2026-09-15` date was restored in `DIM_DAY`.

The validation SQL reports results; it does not yet enforce the proposed `FAIL`, `QUARANTINE`, or `ALERT` actions.

## 5. Reload policy

**Goal:** Reload one affected inventory date without rebuilding the full table.

For the demonstration, date `2026-10-04` was reloaded using source file
`rms/soh_full/ITEM_LOC_SOH_FULL_20261004.csv`.

1. The original Silver table contained **17,440** rows for this date.
2. A temporary table, `INVENTORY_RELOAD_DEMO`, was created as a copy of that date from Silver.
3. One row was deleted from the temporary copy to simulate a problem (**17,439** rows).
4. The date was reloaded from the Bronze source file into the temporary table.
5. Final validation showed **17,440** rows, with **1** row restored.

**Important:** This was a demonstration using a temporary copy. The actual Silver
`INVENTORY_SNAPSHOT` table was not changed. The SQL script demonstrates the
one-day reload steps, but a production reload would need to apply the same
date-specific process to the real target table after validation.

## 6. Retention and replay principles

The submitted SQL does not configure retention periods. Until business retention durations are agreed:
- Preserve Bronze source files and raw records as the replay and audit source.
- Retain Silver lineage and quarantined records so rejected data can be investigated and corrected.
- Retain Gold data according to reporting needs; rebuild corrected date slices from Silver.
- Do not silently delete quarantined records before their issues are resolved.
- Retain Marts reporting datasets according to business reporting requirements.

## 7. Key implementation gaps

1. The submitted Bronze load script does not contain the complete COPY logic for all 64 source files.
2. The submitted Silver transformation script documents inventory processing only; SQL for the other Silver tables is not included.
3. The submitted file-format script defines three formats, while the Bronze load script references an additional format not defined there.
4. Gold currently uses full-table rebuilds; the one-day reload demonstration used a temporary copy and did not change the actual Silver table.
5. SCD Type 2 is not implemented for `DIM_ITEM` or `DIM_LOCN` in the submitted Gold SQL.
6. The quality checks are read-only; proposed gates are not yet enforced.
7. The `MARTS` schema exists, but mart tables are not created.
