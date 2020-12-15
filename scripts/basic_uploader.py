"""
    This script should upload an ensemble with at least two children to sumo using the python wrapper.
    Stored ensemble must be deleted afterwards.
"""
import os
import sys
import yaml
import argparse

try:
    sys.path.index('../src')
except ValueError:
    sys.path.append('../src')

from fmu import sumo


def main():
    manifest_path, search_path, env, threads = parse_arguments()

    sumo_connection = sumo.SumoConnection(env=env)
    e = sumo.EnsembleOnDisk(manifest_path=manifest_path, sumo_connection=sumo_connection)
    e.add_files(search_path + '/*.bin')
    e.upload(threads=threads, showplot=True)


def parse_arguments():
    """

        Parse the arguments

        Returns:
            args: argparse.ArgumentParser() object

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('manifest_path', type=str, help='Absolute path to run manifest (ensemble)')
    parser.add_argument('search_path', type=str, help='Global search path for files to upload')
    parser.add_argument('env', type=str, help="Which environment to use.")
    parser.add_argument('threads', type=int, help="Set number of threads to use.")
    args = parser.parse_args()

    if args.env not in ['dev', 'test', 'prod', 'exp']:
        raise ValueError(f'Illegal environment: {args.env}. Valid environments: dev, test, prod, exp')

    manifest_path = os.path.join(args.manifest_path, 'fmu_ensemble.yaml')

    return manifest_path, args.search_path, args.env, args.threads


if __name__ == '__main__':
    main()
