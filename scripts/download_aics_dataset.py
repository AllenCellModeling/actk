#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import sys
import traceback
from pathlib import Path

import pandas as pd
from lkaccess import LabKey, contexts

###############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)4s: %(module)s:%(lineno)4s %(asctime)s] %(message)s",
)
log = logging.getLogger(__name__)

###############################################################################
# Args


class Args(argparse.Namespace):
    def __init__(self):
        self.__parse()

    def __parse(self):
        # Setup parser
        p = argparse.ArgumentParser(
            prog="download_aics_dataset",
            description=(
                "Retrieve a dataset ready for processing from the internal "
                "AICS database."
            ),
        )

        # Arguments
        p.add_argument(
            "--sample",
            type=float,
            default=1.0,
            help=(
                "Percent how much data to download. Will be split across cell lines. "
                "Ex: 1.0 = 100 percent of each cell line, "
                "0.05 = 5 percent of each cell line."
            ),
        )
        p.add_argument(
            "--instance",
            default="PROD",
            help="Which database instance to use for data retrieval. (PROD or STAGING)",
        )
        p.add_argument(
            "--save_path",
            type=Path,
            default=Path("aics_ic_data.csv"),
            help="Path to save the dataset to.",
        )
        p.add_argument(
            "--debug",
            action="store_true",
            help="Show traceback if the script were to fail.",
        )

        # Parse
        p.parse_args(namespace=self)


###############################################################################
# Retrieve and prepare AICS dataset


def download_aics_dataset(args: Args):
    # Try running the download pipeline
    try:
        # Get instance context
        instance_context = getattr(contexts, args.instance.upper())

        # Create connection to instance
        lk = LabKey(instance_context)
        log.info(f"Using LabKey instance: {lk}")

        # Get integrated cell data
        log.info("Retrieving pipeline integrated cell data...")
        data = pd.DataFrame(lk.dataset.get_pipeline_4_production_data())

        # Get cell line data
        log.info("Retrieving cell line data...")
        cell_line_data = pd.DataFrame(
            lk.select_rows_as_list(
                schema_name="celllines",
                query_name="CellLineDefinition",
                columns=[
                    "CellLineId",
                    "CellLineId/Name",
                    "StructureId/Name",
                    "ProteinId/Name",
                ],
            )
        )

        # Merge the data
        data = data.merge(cell_line_data, how="left", on="CellLineId")

        # Prepare the raw data for sampling
        data = data.drop_duplicates(subset=["CellId"], keep="first")
        data = data.reset_index(drop=True)
        data["CellLineId"] = data["CellLineId"].astype(int)

        # Sample the data
        if args.sample != 1.0:
            log.info(f"Sampling dataset with frac={args.sample}...")
            data = data.groupby("CellLineId", group_keys=False)
            data = data.apply(pd.DataFrame.sample, frac=args.sample)
            data = data.reset_index(drop=True)

        # Rename columns to match DatasetFields
        data = data.rename(columns={
            "ChannelNumber405": DatasetFields.ChannelIndexDNA,
            "ChannelNumber638": DatasetFields.ChannelIndexMembrane,
            "ChannelNumberStruct": DatasetFields.ChannelIndexStructure,
            "ChannelNumberBrightfield": DatasetFields.ChannelIndexBrightfield,
        })

        # Save to CSV
        data.to_csv(args.save_path, index=False)

        log.info(f"Saved dataset manifest to: {args.save_path}")

    # Catch any exception
    except Exception as e:
        log.error("=============================================")
        if args.debug:
            log.error("\n\n" + traceback.format_exc())
            log.error("=============================================")
        log.error("\n\n" + str(e) + "\n")
        log.error("=============================================")
        sys.exit(1)


###############################################################################
# Runner


def main():
    args = Args()
    download_aics_dataset(args)


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)

if __name__ == "__main__":
    main()
