#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import sys
import traceback
from pathlib import Path

from quilt3 import Package
from ack import get_module_version

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
            prog="upload_test_data",
            description=(
                "Upload files used for testing this project. This will upload "
                "whatever files are currently found in the `tests/data` directory. To "
                "add more test files, simply add them to the `tests/data` directory "
                "and rerun this script."
            ),
        )

        # Arguments
        p.add_argument(
            "--dry-run",
            action="store_true",
            help=(
                "Conduct dry run of the package generation. Will create a JSON "
                "manifest file of that package instead of uploading."
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


def upload_test_data(args: Args):
    # Try running the download pipeline
    try:
        # Get test data dir
        data_dir = (
            Path(__file__).parent.parent / "ack" / "tests" / "data"
        ).resolve(strict=True)

        # Report with directory will be used for upload
        log.info(f"Using contents of directory: {data_dir}")

        # Create quilt package
        package = Package()
        package.set_dir("data", data_dir)

        # Report package contents
        log.info(f"Package contents: {package}")

        # Check for dry run
        if args.dry_run:
            # Attempt to build the package
            built = package.build("ack/test_data")

            # Get resolved save path
            manifest_save_path = Path("upload_manifest.jsonl").resolve()
            with open(manifest_save_path, "w") as manifest_write:
                package.dump(manifest_write)

            # Report where manifest was saved
            log.info(f"Dry run generated manifest stored to: {manifest_save_path}")
            log.info(f"Completed package dry run. Result hash: {built.top_hash}")

        # Upload
        else:
            # Get upload confirmation
            confirmation = None
            while confirmation not in ["y", "n"]:
                # Get user input
                confirmation = input("Upload [y]/n? ")

                # Get first character and lowercase
                confirmation = confirmation[0].lower()

            # Check confirmation
            if confirmation == "y":
                pushed = package.push(
                    "ack/test_data",
                    "s3://aics-modeling-packages-test-resources",
                    message=f"Test resources for `ack` version: {get_module_version()}."
                )

                log.info(f"Completed package push. Result hash: {pushed.top_hash}")
            else:
                log.info(f"Upload canceled.")

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
    upload_test_data(args)


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)

if __name__ == "__main__":
    main()
