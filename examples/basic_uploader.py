"""
    This script should upload an ensemble with at least two children to sumo using the python wrapper.
    Stored ensemble must be deleted afterwards.
"""
import os
import sys
import argparse

from fmu.sumo import uploader

def main():
    args = parse_arguments()
    sumo_connection = uploader.SumoConnection(env=args.env)
    e = uploader.EnsembleOnDisk(
        ensemble_metadata_path=args.ensemble_metadata_path,
        sumo_connection=args.sumo_connection
        )
    e.add_files(args.search_path)
    e.upload(threads=args.threads, register_ensemble=True)

def parse_arguments():
    """

        Parse the arguments

        Returns:
            args: argparse.ArgumentParser() object

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('ensemble_metadata_path', type=str, help='Absolute path to ensemble metadata')
    parser.add_argument('search_path', type=str, help='Search path for files to upload')
    parser.add_argument('env', type=str, default='prod', help="Sumo environment. Default: prod")
    parser.add_argument('threads', type=int, default=8, help="Number of threads to use. Default: 8")
    args = parser.parse_args()

    if args.env not in ['dev', 'test', 'prod', 'exp', 'fmu']:
        raise ValueError(f'Illegal environment: {args.env}. Valid environments: dev, test, prod, exp, fmu')

    return args


if __name__ == '__main__':
    main()
