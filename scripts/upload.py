import argparse
from fmu import sumo
import os

def main():
    """Script for uploading data to Sumo"""

    args = parse_arguments()
    manifest_path = os.path.join(args.casepath, 'pred/share/runinfo/manifest.yaml')

    # add some files
    subfolders = ['pred/share/results/maps/depth/*.gri',
                  'pred/share/results/maps/isochores/*.gri',
                  'pred/share/results/maps/depth_conversion/*.gri',
                  'realization-0/pred/share/polygons/field_outline.csv',
                  'realization-0/pred/share/polygons/*--faultlines.csv',
                  'realization-*/pred/share/maps/depth/*.gri',
                  'realization-*/pred/share/maps/isochores/*.gri',
                  #'realization-0/pred/share/maps/depth_conversion/*.gri',
                  #'realization-0/pred/share/maps/depth/*.gri',
                  ]

    for subfolder in subfolders:
        print(subfolder)
        sumo_connection = sumo.SumoConnection(env=args.env)
        e = sumo.EnsembleOnDisk(manifest_path=manifest_path, sumo_connection=sumo_connection)
        e.add_files(os.path.join(args.casepath, subfolder))
    
        print('\nuploading files')
        e.upload(threads=4)

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('casepath', type=str, help='Absolute path to case root on Scratch')
    parser.add_argument('--env', type=str, default='dev', help="Which environment to use, default: dev")
    args = parser.parse_args()

    if args.env not in ['dev', 'test', 'prod']:
        raise ValueError(f'Illegal environment: {args.env}. Valid environments: dev, test, prod')

    return args

if __name__ == '__main__':
    main()