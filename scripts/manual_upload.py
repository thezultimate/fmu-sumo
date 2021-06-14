"""Script for manually uploading data from an FMU case to Sumo"""

import os
import logging
import argparse

from fmu.sumo import uploader

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    """ The main function"""

    args = parse_arguments()
    case_metadata_path = os.path.join(args.casepath, "share/metadata/fmu_case.yml")

    # add some files
    subfolders = [
        f"realization-*/{args.iteration}/share/results/maps/*.gri",
        f"realization-*/{args.iteration}/share/results/polygons/*.csv",
        f"realization-*/{args.iteration}/share/results/tables/*.csv",
    ]

    sumo_connection = uploader.SumoConnection(env=args.env)
    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata_path, sumo_connection=sumo_connection
    )

    e.register()

    for subfolder in subfolders:
        print("Adding files: {}".format(subfolder))
        e.add_files(os.path.join(args.casepath, subfolder))

    logging.info("file count is %s", str(len(e.files)))
    logging.info("thread count is %s", str(args.threads))
    logging.info("environment is %s", str(args.env))

    upload_results = e.upload(threads=args.threads)

    logging.info(upload_results)


def parse_arguments():
    """parse the arguments, return args object"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "casepath", type=str, help="Absolute path to case root on Scratch"
    )
    parser.add_argument(
        "--env", type=str, default="fmu", help="Which environment to use, default: fmu"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Set number of threads to use. Default: 4.",
    )
    parser.add_argument("--iteration", type=str, default="*", help="Iteration")
    args = parser.parse_args()

    if args.env not in ["dev", "test", "prod", "exp", "fmu", "preview"]:
        raise ValueError(
            f"Illegal environment: {args.env}. Valid environments: dev, test, prod, exp, fmu, preview"
        )

    return args


if __name__ == "__main__":
    main()
