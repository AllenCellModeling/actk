#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import sys
import traceback
from pathlib import Path

from quilt3 import Package

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
            prog="download_test_data",
            description=(
                "Download files used for testing this project. This will download "
                "all the required test resources and place them in the `tests/data` "
                "directory."
            ),
        )

        # Arguments
        p.add_argument(
            "--top-hash",
            default=None,
            help=(
                "A specific version of the package to retrieve. Default: latest"
            ),
        )
        p.add_argument(
            "--debug",
            action="store_true",
            help="Show traceback if the script were to fail.",
        )

        # Parse
        p.parse_args(namespace=self)


###############################################################################
# Build package


def download_test_data(args: Args):
    # Try running the download pipeline
    try:
        # Get test data dir
        data_dir = (
            Path(__file__).parent.parent / "ack" / "tests" / "data"
        ).resolve(strict=True)

        # Get quilt package
        package = Package.browse(
            "ack/test_data",
            "s3://aics-modeling-packages-test-resources",
            top_hash=args.top_hash
        )

        # Download
        package["models"].fetch(
            data_dir
        )

        log.info(f"Completed package download.")

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
    download_test_data(args)


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)

if __name__ == "__main__":
    main()
