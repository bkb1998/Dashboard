#!/usr/bin/env python
"""
tx_lottery_etl.py
=================

ETL → Star Schema for the Texas Lottery® Sales dataset
"""

import argparse
from pathlib import Path
import pandas as pd
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Texas Lottery ETL → Star Schema")
    p.add_argument("csv", type=Path, help="Raw Texas Lottery CSV (3.9 GB, 14 M rows)")
    p.add_argument("outdir", type=Path, help="Output directory for CSV files")
    p.add_argument("--chunksize", type=int, default=250_000, help="Rows per chunk")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    raw_file: Path = args.csv.expanduser().resolve()
    out_dir: Path = args.outdir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    chunksize: int = args.chunksize

    # ---------------- Define columns -------------------
    retailer_cols = [
        "Retailer License Number",
        "Retailer Location Name",
        "Retailer Location Address 1",
        "Retailer Location City",
        "Retailer Location State",
        "Retailer Location Zip Code",
        "Owning Entity Retailer Name",
    ]
    game_cols = ["Scratch Game Number", "Game Category", "Ticket Price"]

    numeric_cols = [
        "Net Ticket Sales Amount",
        "Gross Ticket Sales Amount",
        "Promotional Tickets Amount",
        "Cancelled Tickets Amount",
        "Ticket Adjustments Amount",
        "Ticket Returns Amount",
        "Ticket Price",
    ]
    text_cols = [
        "Retailer Location Name",
        "Retailer Location Address 1",
        "Retailer Location City",
        "Retailer Location State",
        "Owning Entity Retailer Name",
        "Game Category",
    ]
    fact_cols = [
        "Row ID",
        "Month Ending Date",
        "Fiscal Year",
        "Fiscal Month",
        "Retailer License Number",
        "Game ID",
    ] + numeric_cols

    # ---------------- Initialize -------------------
    dim_retailer, dim_game = {}, {}
    fact_file = out_dir / "FactTicketSales.csv"
    fact_file.unlink(missing_ok=True)  # remove old file if exists
    first_chunk = True
    all_dates = []

    print(f"🔄  Streaming {raw_file.name} …")
    for chunk in tqdm(
        pd.read_csv(
            raw_file,
            chunksize=chunksize,
            dtype={
                "Retailer License Number": "Int64",
                "Scratch Game Number": "Int64",
                "Ticket Price": "Int16",
            },
            parse_dates=["Month Ending Date"],
            keep_default_na=False,
            low_memory=False,
        ),
        unit="chunk",
    ):
        # --- Drop rows missing required keys
        chunk = chunk.dropna(
            subset=["Row ID", "Month Ending Date", "Retailer License Number"]
        ).copy()

        # --- Numeric cleaning
        chunk[numeric_cols] = (
            chunk[numeric_cols]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
            .astype("float64")
        )

        # --- Text cleaning
        chunk[text_cols] = chunk[text_cols].replace({"": pd.NA}).fillna("Unknown")

        # --- Game ID creation
        chunk["Scratch Game Number"] = chunk["Scratch Game Number"].fillna(0).astype("Int64")
        chunk["Game ID"] = (
            chunk["Scratch Game Number"].astype(str) + "-" + chunk["Game Category"]
        )

        # --- Update dimensions
        dim_retailer.update(
            chunk[retailer_cols]
            .drop_duplicates(subset=["Retailer License Number"])
            .set_index("Retailer License Number")
            .to_dict("index")
        )
        dim_game.update(
            chunk[["Game ID"] + game_cols]
            .drop_duplicates(subset=["Game ID"])
            .set_index("Game ID")
            .to_dict("index")
        )

        # --- Append fact chunk directly to file
        chunk[fact_cols].to_csv(
            fact_file,
            mode="w" if first_chunk else "a",
            header=first_chunk,
            index=False,
        )
        first_chunk = False
        all_dates.append(chunk["Month Ending Date"].dropna().min())
        all_dates.append(chunk["Month Ending Date"].dropna().max())

    # ---------------- Write Dimensions -------------------
    print("💾  Writing DimRetailer.csv …")
    (
        pd.DataFrame.from_dict(dim_retailer, orient="index")
        .reset_index()
        .rename(columns={"index": "Retailer License Number"})
        .to_csv(out_dir / "DimRetailer.csv", index=False)
    )

    print("💾  Writing DimGame.csv …")
    (
        pd.DataFrame.from_dict(dim_game, orient="index")
        .reset_index()
        .rename(columns={"index": "Game ID"})
        .to_csv(out_dir / "DimGame.csv", index=False)
    )

    # ---------------- DimDate -------------------
    print("💾  Building DimDate.csv …")
    min_date = pd.to_datetime(min(all_dates))
    max_date = pd.to_datetime(max(all_dates))
    date_range = pd.date_range(start=min_date, end=max_date, freq="D")
    dim_date = pd.DataFrame({"Date": date_range})
    dim_date["Fiscal Year"] = dim_date["Date"].dt.year
    dim_date["Fiscal Month"] = dim_date["Date"].dt.month
    dim_date["Calendar Year"] = dim_date["Date"].dt.year
    dim_date["Calendar Month"] = dim_date["Date"].dt.month
    dim_date.to_csv(out_dir / "DimDate.csv", index=False)

    print(f"✅  ETL finished. Files saved to: {out_dir}\n")


if __name__ == "__main__":
    main()
