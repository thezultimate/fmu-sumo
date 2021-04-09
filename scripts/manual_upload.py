import argparse
from fmu.sumo import uploader
import os
import logging
import sys
from datetime import datetime
import time

logger = logging.getLogger()
#logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

def main():
    """Script for uploading data to Sumo"""

    args = parse_arguments()
    ensemble_metadata_path = os.path.join(args.casepath, f'{args.iteration}/share/metadata/fmu_ensemble.yml')

    # add some files
    subfolders = [f'realization-0/{args.iteration}/share/results/maps/*.gri',
                  #f'realization-*/{args.iteration}/share/results/polygons_metadata_demo/*.pol',
                  #f'realization-*/{args.iteration}/share/results/maps_metadata_demo/*amplitude*.gri',
                  #f'realization-*/{args.iteration}/share/results/tables_metadata_demo/inplace_volumes*.csv',

                  #f'{args.iteration}/share/results/maps/depth/*.gri',
                  #f'{args.iteration}/share/results/maps/isochores/*.gri',
                  #f'{args.iteration}/share/results/maps/depth_conversion/*.gri',
                  #f'realization-0/{args.iteration}/share/results/polygons/*--field_outline.csv',
                  #f'realization-0/{args.iteration}/share/results/polygons/*--faultlines.csv',
                  #f'realization-*/{args.iteration}/share/results/maps/depth/*.gri',
                  #f'realization-*/{args.iteration}/share/results/maps/isochores/*.gri',
                  #f'realization-0/{args.iteration}/share/results/maps/fwl/*.gri'
                  #f'realization-0/{args.iteration}/share/results/maps/depth_conversion/*.gri',
                  #f'realization-0/{args.iteration}/share/results/maps/depth/*.gri',
                  #f'realization-0/{args.iteration}/share/results/maps/isochores/*.gri',
                  ]

    sumo_connection = uploader.SumoConnection(env=args.env)
    e = uploader.EnsembleOnDisk(ensemble_metadata_path=ensemble_metadata_path, sumo_connection=sumo_connection)

    e.register()

    for subfolder in subfolders:
        print('Adding files: {}'.format(subfolder))
        e.add_files(os.path.join(args.casepath, subfolder))

    print(f'{datetime.isoformat(datetime.now())}: Uploading {len(e.files)} files with {args.threads} threads on environment {args.env}')
    upload_results = e.upload(threads=args.threads)

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('casepath', type=str, help='Absolute path to case root on Scratch')
    parser.add_argument('--env', type=str, default='fmu', help="Which environment to use, default: dev")
    parser.add_argument('--threads', type=int, default=4, help="Set number of threads to use. Default: 4.")
    parser.add_argument('--iteration', type=str, default='iter-0', help="Iteration")
    args = parser.parse_args()

    if args.env not in ['dev', 'test', 'prod', 'exp', 'fmu']:
        raise ValueError(f'Illegal environment: {args.env}. Valid environments: dev, test, prod, exp, fmu')

    return args

if __name__ == '__main__':
    main()
