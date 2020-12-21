import argparse
from fmu import sumo
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
    manifest_path = os.path.join(args.casepath, 'pred/share/runinfo/manifest.yaml')

    # add some files
    subfolders = [#'pred/share/results/maps/depth/*.gri',
                  #'pred/share/results/maps/isochores/*.gri',
                  #'pred/share/results/maps/depth_conversion/*.gri',
                  'realization-0/pred/share/results/polygons/*--field_outline.csv',
                  'realization-0/pred/share/results/polygons/*--faultlines.csv',
                  #'realization-*/pred/share/maps/depth/*.gri',
                  #'realization-*/pred/share/maps/isochores/*.gri',
                  #'realization-0/pred/share/maps/fwl/*.gri'
                  #'realization-0/pred/share/maps/depth_conversion/*.gri',
                  'realization-0/pred/share/results/maps/depth/*.gri',
                  'realization-0/pred/share/results/maps/isochores/*.gri',
                  ]

    sumo_connection = sumo.SumoConnection(env=args.env)
    e = sumo.EnsembleOnDisk(manifest_path=manifest_path, sumo_connection=sumo_connection)

    for subfolder in subfolders:
        print('Adding files: {}'.format(subfolder))
        e.add_files(os.path.join(args.casepath, subfolder))
    
    _t0 = time.perf_counter()
    print(f'{datetime.isoformat(datetime.now())}: Uploading {len(e.files)} files with {args.threads} threads on environment {args.env}')
    upload_results = e.upload(threads=args.threads, showplot=False)
    _dt = time.perf_counter() - _t0
    print(f"Total: {len(e.files)}" \
        f"\nOK: {len(upload_results.get('ok_uploads'))}" \
        f"\nFailures: {len(upload_results.get('failed_uploads'))}" \
        f"\nRejected: {len(upload_results.get('rejected_uploads'))}" \
        f"\nWall time: {round(_dt,2)} seconds" \
        )

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('casepath', type=str, help='Absolute path to case root on Scratch')
    parser.add_argument('--env', type=str, default='dev', help="Which environment to use, default: dev")
    parser.add_argument('--threads', type=int, default=4, help="Set number of threads to use. Default: 4.")
    args = parser.parse_args()

    if args.env not in ['dev', 'test', 'prod', 'exp']:
        raise ValueError(f'Illegal environment: {args.env}. Valid environments: dev, test, prod, exp')

    return args

if __name__ == '__main__':
    main()