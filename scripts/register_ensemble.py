import argparse
from fmu.sumo import uploader

"""

    Script for registering an ensemble on Sumo, intended to be run as an ERT pre-sim HOOK workflow.

    - Contact Sumo, confirm that the connection is OK. Post information in the shell making the user go through authentication if not??
    - Parse the ensemble_metadata. 
       - Get the fmu-id from the ensemble_metadata, search Sumo, figure out if it has been uploaded already. If so, pass. If not, register it. Print information.
       - Sumo should perhaps just register it anyways, will simply update if not present.

       Discussions for later: Could be an option to include the sumo_id in the ensemble_metadata, along with some information about when it was registered etc, but
                              there are several difficulties and weird edge-cases that makes this complicated.
                              - If user uploads ensemble, then deletes it from Sumo, then wants to re-upload it. It will get the same sumo_id.
                              - If user uploads ensemble to DEV, then to PROD, this will break down? So need to include environment then.

                              For now: Only relate to the fmu_id FMU-side.
    - Register the ensemble on Sumo. Print information.

"""

def main():

    # parse arguments
    args = parse_arguments()

    # establish the connection
    sumo_connection = uploader.SumoConnection(env=args.env)

    # initiate the ensemble on disk object. This will also register the ensemble on Sumo.
    e = uploader.EnsembleOnDisk(ensemble_metadata_path=args.ensemble_metadata_path, sumo_connection=sumo_connection)

    # Register the ensemble on Sumo
    e.register()

    # make some space in the output
    print("\n\n")

def parse_arguments():

    """

        Parse the arguments

        Returns:
            args: argparse.ArgumentParser() object

    """

    parser = argparse.ArgumentParser()
    parser.add_argument('ensemble_metadata_path', type=str, help='Absolute path to run ensemble_metadata')
    parser.add_argument('env', type=str, help="Which environment to use.")
    args = parser.parse_args()

    if args.env not in ['dev', 'test', 'prod', 'exp', 'fmu', 'preview']:
        raise ValueError(f'Illegal environment: {args.env}. Valid environments: dev, test, prod, exp, preview')

    return args


if __name__ == '__main__':
    main()
