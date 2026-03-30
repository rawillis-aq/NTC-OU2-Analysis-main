"""
Data loader module for plotting routines
Newtown Creek, Operational Unit Two

This module imports and pre-processes surface water data for RI,

todo:
- After Wednesday
  - saltwater pumphouse, scs, and PDI/treatability study need to be included
  - convert spatial sediment (SCS) plot to Python
  - figure out what decomposable temporal trends
  - D/F non-detects not classified
  - verify stream mile is integrated correctly
- All totals (PCB, DF, PAH; not-KM) included as a flag for OU2
- Figure out whether aliphatics are adjusted (usually unadjusted; need to rename to match)
- Consider standardizing chemical name; only CAS_RN is standardized for now
"""

from __future__ import annotations

import os
import time
import warnings
import pickle
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from aqpy.io.aqsql import aqSQL as aqsql
from aqpy.io.odbc_connect import sql_connect
from dotenv import load_dotenv

load_dotenv()

warnings.filterwarnings("ignore", category=UserWarning, module="aqpy.io.aqsql")

_user_name = os.environ.get("DB_USERNAME")
_pwd = os.environ.get("DB_PWD")
_server = os.environ.get("DB_SERVER")
_database = os.environ.get("DB_RPT")


def main() -> None:
    """Run data loader and save QC output to disk."""
    load_data(refresh=True, write=True, qc=True)


def out2_interim(df):
    """This function contains placeholder overrides for the OU2 dataset. Delete or migrate when ready."""

    # stream mile and river mile to be populated by DSG
    df["STREAM_MILE"] = 0
    df["MILES_FROM_NC_MOUTH"] = 0

    # remove nonstandard totals, leaving U = 1/2 MDL totals; to be replaced with KM totals by DSG
    mask = (df["RESULT_TYPE_CODE"] != "CALC") | (
        (df["RESULT_TYPE_CODE"] == "CALC")
        & df["CHEMICAL_NAME"]
        .astype(str)
        .str.contains(" (MDL) (U = 1/2 max limit)", regex=False, na=False)
    )
    df = df[mask].copy()

    # standardize aliphatics to unadjusted CAS RN; DSG looking into whether actually unadjusted
    mask = df["CAS_RN"] == "ALIPHATIC19-36"
    df.loc[mask, "CAS_RN"] = "Aliphatic19-36U"

    return df


def load_data(refresh=False, write=False, qc=False):
    """Run data loader and return the result."""

    keep_columns = [
        "TASK_CODE",
        "TASK_NAME",
        "EVENT",
        "SYS_LOC_CODE",
        "STREAM_MILE",
        "MILES_FROM_NC_MOUTH",
        "X_COORD",
        "Y_COORD",
        "COORD_TYPE_CODE",
        "ELEV",
        "ELEV_UNIT",
        "SYS_SAMPLE_CODE",
        "MATRIX_CODE",
        "SAMPLE_DATE",
        "START_DEPTH",
        "END_DEPTH",
        "DEPTH_UNIT",
        "GROUP_DESC",
        "CHEMICAL_NAME",
        "CAS_RN",
        "FRACTION",
        "ANALYTIC_METHOD",
        "RESULT_VALUE",
        "DETECT_FLAG",
        "TARGET_UNIT",
        "METHOD_DETECTION_LIMIT",
        "REPORTING_DETECTION_LIMIT",
    ]

    pickle_dir = Path("cache")
    pickle_name = "data.pkl"
    pickle_dir.mkdir(parents=False, exist_ok=True)
    pickle_exists = os.path.exists(os.path.join(pickle_dir, pickle_name))
    if not refresh and pickle_exists:
        with open(os.path.join(pickle_dir, pickle_name), "rb") as file:
            df = pickle.load(file)
        return df

    con = sql_connect(_server, _user_name, _pwd, connection=_database)

    output_dir = Path("qc_output")
    output_dir.mkdir(parents=False, exist_ok=True)

    runs = [
        ("OU2", "events"),
        ("SDC", "events"),
        ("RI", "events"),
    ]

    df = pd.DataFrame()

    for dataset, data_type in runs:
        print(f"\n--- {dataset} / {data_type} ---")
        df_raw = read_databases(con, dataset=dataset, data_type=data_type)
        df_processed = process_data(df_raw, dataset=dataset, data_type=data_type)

        if qc:
            make_qc_plot(df_processed, dataset=dataset, output_dir=output_dir)

        if write:
            output_file = output_dir / f"{dataset.lower()}_{data_type}_qc.csv"
            df_processed.to_csv(output_file, index=False)

            print(f"Wrote {len(df_processed):,} rows to {output_file}")

        df_processed = df_processed[keep_columns]

        df = pd.concat([df, df_processed], ignore_index=True, axis=0)

    df.to_pickle(os.path.join(pickle_dir, pickle_name))

    if write:
        output_file = output_dir / f"df_qc.csv"
        df.to_csv(output_file, index=False)

        print(f"Wrote {len(df):,} rows to {output_file}")

    return df


def try_sql_connect(i, db, table, where=None, max_retries=5):
    """Attempt to retrieve data from SQL with retry logic."""
    try:
        return db.select(table, columns=[], where=where)
    except Exception as error:
        if i < max_retries:
            print("Failed to load data connection... retrying...")
            time.sleep(1)
            return try_sql_connect(i + 1, db, table, where, max_retries)

        print("Maximum retry attempts exceeded.")
        raise error


def read_databases(con, dataset: str, data_type: str) -> pd.DataFrame:
    """Load raw data for a dataset/data_type combination."""
    dataset = dataset.upper()
    data_type = data_type.lower()

    if data_type != "events":
        raise ValueError(
            f"Only data_type='events' is supported right now, got {data_type!r}"
        )

    if dataset == "SDC":
        print("Reading SDC surface water data from EQuIS...")
        db = aqsql(con, 52)  # 52: newtown
        table = "aq_Newtown_ReportingDB_vw"
        where = {"task_code": "NCFS_SDC_SW"}
        df = try_sql_connect(0, db, table, where=where)
        return df

    if dataset == "RI":
        print("Reading RI East River surface water files...")

        dir_ri = (
            r"\\wcl-fs1\woodcliff\Projects\Newtown_Creek\RI-FS\Model\Interim_Modeling"
            r"\2020\supporting_data_sources\sources\East_River"
        )
        fn_ri1 = r"E-River_Particulates_20220531.xlsx"
        fn_ri2 = (
            r"calc_particulate_C19C36_DFTEQ"
            r"\ER_surfacewater_processed_db_for_LTE_particulates_20240328_cfo_stats.xlsx"
        )
        fn_ri3 = r"NCP2_SurfaceWater_wKM20161222_TOC_QC.xlsx"

        df_ri1 = pd.read_excel(os.path.join(dir_ri, fn_ri1), sheet_name="ER")
        print("Read RI file 1")

        df_ri2 = pd.read_excel(
            os.path.join(dir_ri, fn_ri2),
            sheet_name="ER_surfacewater_processed_db_fo",
        )
        print("Read RI file 2")

        df_ri3 = pd.read_excel(
            os.path.join(dir_ri, fn_ri3),
            sheet_name="NCP2_SurfaceWater_wKM20161222",
        )
        print("Read RI file 3")

        # tag sources so process_data knows what to do
        df_ri1["RI_SOURCE"] = "RI1"
        df_ri2["RI_SOURCE"] = "RI2"
        df_ri3["RI_SOURCE"] = "RI3"

        return pd.concat([df_ri1, df_ri2, df_ri3], ignore_index=True, sort=False)

    if dataset == "OU2":
        print("Reading OU2 converted data file...")

        ou2_file = Path(
            r"\\fuji\Anchor\Projects\Newtown_Creek\DATA_MART\NYC_OU2\NC_OU2_Converted20260312.xlsx"
        )
        df_ou2 = pd.read_excel(ou2_file, sheet_name="Sheet1")

        return df_ou2

    raise ValueError(
        f"Unsupported dataset/data_type combination: {dataset}, {data_type}"
    )


def process_data(df: pd.DataFrame, dataset: str, data_type: str) -> pd.DataFrame:
    """Process raw data for a dataset/data_type combination."""
    dataset = dataset.upper()
    data_type = data_type.lower()

    if data_type != "events":
        raise ValueError(
            f"Only data_type='events' is supported right now, got {data_type!r}"
        )

    df = df.copy()
    df.columns = [col.upper() for col in df.columns]

    if dataset == "SDC":

        # clean up data types
        df["RESULT_VALUE"] = pd.to_numeric(df["RESULT_VALUE"], errors="coerce")
        df["METHOD_DETECTION_LIMIT"] = pd.to_numeric(
            df["METHOD_DETECTION_LIMIT"], errors="coerce"
        )

        # remove field duplicates
        df = df[df["SAMPLE_TYPE_CODE"] == "N"].copy()

        # standardize units name
        df.rename(columns={"RESULT_UNIT": "TARGET_UNIT"}, inplace=True)

        # fill non-detects with MDL for non-calculated results
        mask = (df["RESULT_TYPE_CODE"] != "CALC") & (df["DETECT_FLAG"] == "N")
        df.loc[mask, "RESULT_VALUE"] = df.loc[mask, "METHOD_DETECTION_LIMIT"]

        # keep non-calcs, KM totals, and tDioxFurM_N
        mask = (
            (df["RESULT_TYPE_CODE"] != "CALC")
            | (
                (df["RESULT_TYPE_CODE"] == "CALC")
                & (df["CAS_RN"].str.contains("_KM_MDL", regex=False, na=False))
            )
            | (df["CAS_RN"] == "tDioxFurM_N")
        )
        df = df[mask].copy()

        # remove KM suffix so naming aligns with RI
        df["CAS_RN"] = df["CAS_RN"].str.replace("_KM_MDL", "", regex=False)
        df["CHEMICAL_NAME"] = df["CHEMICAL_NAME"].str.replace(
            " (KM) (MDL)", "", regex=False
        )

        # force matches
        df.loc[df["CAS_RN"] == "tPAH_17NC", "CAS_RN"] = "tPAH_17"
        df.loc[df["CAS_RN"] == "tPAH_17NC_LM", "CAS_RN"] = "tPAH_17_LM"
        df.loc[df["CAS_RN"] == "tPAH_17NC_HM", "CAS_RN"] = "tPAH_17_HM"

        # unit conversions
        mask = (df["CAS_RN"] == "tPAH_34_NC") & (df["TARGET_UNIT"] == "ug/kg")
        df.loc[mask, "RESULT_VALUE"] = df.loc[mask, "RESULT_VALUE"] / 1000.0
        df.loc[mask, "TARGET_UNIT"] = "mg/kg"

        mask = (df["CAS_RN"] == "tPCBCong") & (df["TARGET_UNIT"] == "ng/kg")
        df.loc[mask, "RESULT_VALUE"] = df.loc[mask, "RESULT_VALUE"] / 1_000_000.0
        df.loc[mask, "TARGET_UNIT"] = "mg/kg"

        # sampling event number
        df["SAMPLING_EVENT_NUMBER"] = (
            df["TASK_CODE_2"].astype(str).str.replace("Event ", "", regex=False)
        )
        df["SAMPLING_EVENT_NUMBER"] = pd.to_numeric(
            df["SAMPLING_EVENT_NUMBER"], errors="coerce"
        )
        df["EVENT"] = df["TASK_CODE_2"]

        # standardize particulate PAH group description
        mask = (
            df["GROUP_DESC"]
            == "Polycyclic Aromatic Hydrocarbons (particulate solid)  (ug/kg)"
        ) | (
            df["GROUP_DESC"]
            == "Polycyclic Aromatic Hydrocarbons (Particulate Solid)  (ug/kg)"
        )
        df.loc[mask, "GROUP_DESC"] = (
            "Polycyclic Aromatic Hydrocarbons (Particulate Solid) (ug/kg)"
        )

        # pull TSS for weighted-average work
        tss = df[df["CAS_RN"] == "TSS"].copy()
        tss = validate_tss_merge_input(tss, dataset)
        df = df.merge(tss, how="left", on="SYS_SAMPLE_CODE")

        # standardize task name
        df["TASK_NAME"] = "SDC Data"

    elif dataset == "RI":
        df_ri1 = df[df["RI_SOURCE"] == "RI1"].copy()
        df_ri2 = df[df["RI_SOURCE"] == "RI2"].copy()
        df_ri3 = df[df["RI_SOURCE"] == "RI3"].copy()

        df_ri1 = df_ri1[df_ri1["RESULT_VALUE"] > 0].copy()

        df_ri2 = df_ri2[df_ri2["ZERO FLAG"].isna()].copy()
        df_ri2 = df_ri2[df_ri2["CAS_RN"] != "Aliphatic19-36U"].copy()

        df_ri3_out = pd.DataFrame()

        mask = (
            (df_ri3["TASK_NAME"] == "East River Surface Water Sampling")
            & (df_ri3["BASELINE_RA_USABILITY"] == 1)
            & (df_ri3["USABILITY_HIERARCHY"] == 1)
            & (
                df_ri3["CHEMICAL_NAME"].isin(
                    ["Particulate organic carbon (POC)", "Total suspended solids"]
                )
            )
        )
        df_ri3 = df_ri3[mask].copy()

        df_ri3_small = df_ri3[
            [
                "SUBFACILITY_CODE",
                "SYS_LOC_CODE",
                "SYS_SAMPLE_CODE",
                "MILES_FROM_NC_MOUTH",
                "GROUP_DESC",
                "CHEMICAL_NAME",
                "RESULT_VALUE",
            ]
        ].copy()

        df_ri3_poc_detects = df_ri3[
            df_ri3["CHEMICAL_NAME"] == "Particulate organic carbon (POC)"
        ][["SYS_SAMPLE_CODE", "DETECT_FLAG"]].copy()

        df_ri3_small = df_ri3_small.pivot(
            index=[
                "SUBFACILITY_CODE",
                "SYS_LOC_CODE",
                "SYS_SAMPLE_CODE",
                "MILES_FROM_NC_MOUTH",
                "GROUP_DESC",
            ],
            columns="CHEMICAL_NAME",
            values="RESULT_VALUE",
        ).reset_index()

        df_ri3_small["RESULT_VALUE"] = (
            100
            * df_ri3_small["Particulate organic carbon (POC)"]
            / df_ri3_small["Total suspended solids"]
        )
        df_ri3_small["CHEMICAL_NAME"] = "Total Organic Carbon"
        df_ri3_small["CAS_RN"] = "TOC"
        df_ri3_small["TASK_NAME"] = "RI Suspended Solids"
        df_ri3_small["TASK_CODE"] = "RI Suspended Solids"
        df_ri3_small["TARGET_UNIT"] = "pct"

        df_ri3_out = df_ri3_small.merge(
            df_ri3_poc_detects,
            how="left",
            on="SYS_SAMPLE_CODE",
        )

        df = pd.concat([df_ri1, df_ri2, df_ri3_out], ignore_index=True, sort=False)

        # Fix CAS numbers that Excel converted to dates
        df["CAS_RN"] = df["CAS_RN"].astype(str)
        df.loc[df["CAS_RN"].str.contains("7440-09-07", na=False), "CAS_RN"] = (
            "7440-09-7"
        )

        # convert D/F to ng/kg
        mask = (df["CAS_RN"] == "tDioxFurM") & (df["TARGET_UNIT"] == "mg/kg")
        df.loc[mask, "RESULT_VALUE"] = df.loc[mask, "RESULT_VALUE"] * 1_000_000.0
        df.loc[mask, "TARGET_UNIT"] = "ng/kg"

        # standardize task naming
        df["TASK_NAME_ORIG"] = df["TASK_NAME"]
        df["TASK_NAME_CODE"] = df["TASK_CODE"]

        df["TASK_NAME"] = "RI Suspended Solids"
        df["TASK_CODE"] = "RI Suspended Solids"

        # standardize river mile name
        df = df.rename(columns={"RIVER_STREAM_MILE": "STREAM_MILE"})

        # remove entries not in study area
        mask = df["AREA"] == "Study Area"
        df = df[mask].reset_index(drop=True)

    elif dataset == "OU2":

        # apply overrides
        df = out2_interim(df)

        # clean up data types
        df["RESULT_VALUE"] = pd.to_numeric(df["RESULT_VALUE"], errors="coerce")
        df["METHOD_DETECTION_LIMIT"] = pd.to_numeric(
            df["METHOD_DETECTION_LIMIT"], errors="coerce"
        )

        # remove field duplicates
        df = df[df["SAMPLE_TYPE_CODE"] == "N"].copy()

        # standardize units column
        df.rename(columns={"RESULT_UNIT": "TARGET_UNIT"}, inplace=True)

        # fill non-detects with MDL for non-calculated results
        mask = (df["RESULT_TYPE_CODE"] != "CALC") & (df["DETECT_FLAG"] == "N")
        df.loc[mask, "RESULT_VALUE"] = df.loc[mask, "METHOD_DETECTION_LIMIT"]

        # remove OU2 MDL naming so it aligns with RI/SDC
        df["CAS_RN"] = df["CAS_RN"].astype(str).str.replace("_MDL_N", "", regex=False)
        df["CHEMICAL_NAME"] = (
            df["CHEMICAL_NAME"]
            .astype(str)
            .str.replace(" (MDL) (U = 1/2 max limit)", " (U=1/2) (MDL)", regex=False)
        )

        # force matches
        df.loc[df["CAS_RN"] == "tPAH_17NC", "CAS_RN"] = "tPAH_17"
        df.loc[df["CAS_RN"] == "tPAH_17NC_LM", "CAS_RN"] = "tPAH_17_LM"
        df.loc[df["CAS_RN"] == "tPAH_17NC_HM", "CAS_RN"] = "tPAH_17_HM"

        # unit conversions
        mask = (df["CAS_RN"] == "tPAH_34_NC") & (df["TARGET_UNIT"] == "ug/kg")
        df.loc[mask, "RESULT_VALUE"] = df.loc[mask, "RESULT_VALUE"] / 1000.0
        df.loc[mask, "TARGET_UNIT"] = "mg/kg"

        mask = (df["CAS_RN"] == "tPCBCong") & (df["TARGET_UNIT"] == "ng/kg")
        df.loc[mask, "RESULT_VALUE"] = df.loc[mask, "RESULT_VALUE"] / 1_000_000.0
        df.loc[mask, "TARGET_UNIT"] = "mg/kg"

        # use sample_date as event identifier
        df["SAMPLE_DATE"] = pd.to_datetime(df["SAMPLE_DATE"], errors="coerce")
        df["EVENT_KEY"] = df["SAMPLE_DATE"].dt.strftime("%Y-%m")

        event_map = {
            "2024-08": "Event 1",
            "2024-11": "Event 2",
            "2025-03": "Event 3",
            "2025-07": "Event 4",
            "2025-09": "Event 5",
        }
        df["EVENT"] = df["EVENT_KEY"].map(event_map)
        mask = df["TASK_CODE_2"] == "NYC_OU2_NC"
        df.loc[mask, "EVENT"] = "OU2 Point Sources"

        # merge TSS values for weighted averages
        tss = df[df["CAS_RN"] == "TSS"].copy()
        tss = validate_tss_merge_input(tss, dataset)
        df = df.merge(tss, how="left", on="SYS_SAMPLE_CODE")

        df["TASK_NAME"] = "OU2 Data"

    else:
        raise ValueError(
            f"Unsupported dataset/data_type combination: {dataset}, {data_type}"
        )

    # standardize aliphatics name
    mask = df["CAS_RN"] == "Aliphatic19-36U"
    df.loc[mask, "CHEMICAL_NAME"] = "C19-C36"

    return df


def make_qc_plot(
    df: pd.DataFrame,
    dataset: str,
    output_dir: Path,
    chem: str | None = None,
) -> None:
    """Make a quick QC plot for one dataset."""

    if "CAS_RN" not in df.columns or "RESULT_VALUE" not in df.columns:
        print(f"Skipping QC plot for {dataset}: required columns not found.")
        return

    # pick a default analyte if one is not provided
    default_chems = {
        "SDC": "TSS",
        "RI": "7440-50-8",  # Copper
        "OU2": "PCB-065",
    }
    chem = chem or default_chems.get(dataset.upper())

    if chem is None:
        print(f"Skipping QC plot for {dataset}: no default analyte defined.")
        return

    sub = df[df["CAS_RN"].astype(str) == str(chem)].copy()

    print(f"{dataset} rows for {chem}: {len(sub)}")
    print(sub[["CAS_RN", "RESULT_VALUE"]].head(10))

    if sub.empty:
        print(f"Skipping QC plot for {dataset}: no rows found for {chem}.")
        return

    sub["RESULT_VALUE"] = pd.to_numeric(sub["RESULT_VALUE"], errors="coerce")
    sub = sub[sub["RESULT_VALUE"].notna()].copy()

    if sub.empty:
        print(f"Skipping QC plot for {dataset}: no numeric RESULT_VALUE for {chem}.")
        return

    plt.figure(figsize=(8, 5))
    plt.hist(sub["RESULT_VALUE"], bins=30)
    plt.xlabel("RESULT_VALUE")
    plt.ylabel("Count")
    plt.title(f"{dataset} QC Histogram: {chem}")
    plt.tight_layout()

    output_file = output_dir / f"{dataset.lower()}_{str(chem).replace('/', '_')}_qc.png"
    plt.savefig(output_file, dpi=300)
    plt.close()

    print(f"Wrote QC plot to {output_file}")


def validate_tss_merge_input(tss: pd.DataFrame, dataset: str) -> pd.DataFrame:
    """Validate TSS rows before joining them back to the main dataset."""

    unit_values = tss["TARGET_UNIT"].dropna().unique()
    if len(unit_values) == 0:
        raise ValueError("TSS rows found, but no TARGET_UNIT values are present.")
    if len(unit_values) != 1 or unit_values[0] != "mg/L":
        raise ValueError(
            f"{dataset} TSS units must be exactly one unique value of 'mg/L'; found {list(unit_values)!r}"
        )

    counts = tss.groupby("SYS_SAMPLE_CODE").size()
    duplicate_ids = counts[counts != 1]
    if not duplicate_ids.empty:
        raise ValueError(
            f"{dataset} TSS merge requires exactly one row per SYS_SAMPLE_CODE; found duplicates for {duplicate_ids.index.tolist()[:10]!r}"
        )

    return tss[["SYS_SAMPLE_CODE", "RESULT_VALUE", "DETECT_FLAG"]].rename(
        columns={
            "RESULT_VALUE": "RESULT_VALUE_TSS_MGL",
            "DETECT_FLAG": "DETECT_FLAG_TSS",
        }
    )


if __name__ == "__main__":
    main()
