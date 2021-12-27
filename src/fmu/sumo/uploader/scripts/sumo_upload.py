#!/usr/bin/env python

"""This script uploads data to Sumo from an ERT FORWARD_JOB."""

import warnings
import os
import argparse
import logging
from pathlib import Path

from fmu.sumo import uploader


logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)

# This documentation is for sumo_uploader as an ERT workflow
DESCRIPTION = """
SUMO_UPLOAD will upload files to Sumo. The typical use case is as add-on to 
post-processing workflows which already create data across an ensemble.
"""

EXAMPLES = """
In an existing workflow e.g. ``ert/bin/workflows/MY_WORKFLOW`` with the contents::
  MY_JOB <arguments>
  SUMO_UPLOAD <SCRATCH>/<USER>/<CASE> <SCRATCH>/<USER>/<CASE>/MyIteration/share/results/tables/*.csv
...where ``MY_JOB`` typically refers to a post-processing job creating data.
Note that ERT workflows have no concept of "iteration", which in practice means you must
either update the workflow manually or create one per iteration.
"""  # noqa


def main() -> None:
    """Entry point from command line"""
    args = parse_arguments()

    if args.verbose:
        logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    logger.info("Arguments are: %s", str(vars(args)))

    sumo_upload_main(
        casepath=args.casepath,
        metadata_path=args.metadata_path,
        searchpath=args.searchpath,
        threads=args.threads,
        env=args.env,
    )


def sumo_upload_main(
    casepath: str, metadata_path: str, searchpath: str, threads: int, env: str
):
    """The main script."""

    logger.info("Running fmu_uploader_main() from main()")

    # establish the connection to Sumo
    sumo_connection = uploader.SumoConnection(env=env)
    logger.debug("Connection to Sumo established")

    # initiate the case on disk object
    logger.debug("Case-relative metadata path is %s", metadata_path)
    case_metadata_path = Path(casepath) / Path(metadata_path)
    logger.debug("case_metadata_path is %s", case_metadata_path)
    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata_path, sumo_connection=sumo_connection
    )

    # add files to the case on disk object
    logger.debug("Adding files. Search path is %s", searchpath)
    e.add_files(searchpath)
    logger.debug("%s files has been added", str(len(e.files)))

    if len(e.files) == 0:
        logger.debug("%s No files - aborting")
        warnings.warn("No files found - aborting ")
        return

    # upload the indexed files
    logger.debug("Starting upload")
    e.upload(
        threads=threads, register_case=False
    )  # registration should have been done by HOOK workflow
    logger.debug("upload done")


def parse_arguments():
    """

    Parse the arguments

    Returns:
        args: argparse.ArgumentParser() object

    """

    parser = argparse.ArgumentParser()
    parser.add_argument("casepath", type=str, help="Absolute path to case root")
    parser.add_argument(
        "searchpath", type=str, help="Case-relative search path for files to upload"
    )
    parser.add_argument(
        "env", type=str, help="Which environment to use.", default="prod"
    )
    parser.add_argument(
        "--threads", type=int, help="Set number of threads to use.", default=2
    )
    parser.add_argument(
        "--metadata_path",
        type=str,
        help="Case-relative path to case metadata",
        default="share/metadata/fmu_case.yml",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--debug", action="store_true", help="Debug output, more verbose than --verbose"
    )

    args = parser.parse_args()

    args.casepath = os.path.expandvars(args.casepath)
    args.searchpath = os.path.expandvars(args.searchpath)

    if args.env not in ["dev", "test", "prod", "exp", "preview"]:
        logger.error("env arg was %s", args.env)
        raise ValueError(
            f"Illegal environment: {args.env}. Valid environments: dev, test, prod, exp, preview"
        )

    if not Path(args.casepath).is_absolute():
        logger.error("casepath arg was %s", args.casepath)
        if args.casepath.startswith("<") and args.casepath.endswith(">"):
            ValueError("ERT variable is not defined: %s", args.casepath)
        raise ValueError("Provided casepath must be an absolute path to the case root")

    if not Path(args.casepath).exists():
        logger.error("casepath arg was %s", args.casepath)
        raise ValueError("Provided case path does not exist")

    return args


if __name__ == "__main__":
    main()
