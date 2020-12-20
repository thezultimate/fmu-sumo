import os
import glob
import yaml
import time
import logging
import warnings
import pandas as pd

from fmu.sumo._fileondisk import FileOnDisk
from fmu.sumo._upload_files import UPLOAD_FILES


class EnsembleOnDisk:
    """
    Class to hold information about an ERT run on disk.

    The EnsembleOnDisk object is a representation of files belonging to an ERT ensemble,
    as they are stored on the Scratch disk.

    An Ensemble in this context is a set of metadata describing this particular ensemble (manifest),
    and an arbitrary number of files belonging to this ensemble. Each file is in reality a file pair,
    consisting of a data file (could be any file type) and a metadata file (yaml formatted, according)
    to FMU standards.

    Example for initialization:
        >>> from fmu import sumo
        >>> sumo_connection = sumo.SumoConnection(env=args.env)
        >>> e = sumo.EnsembleOnDisk(manifest_path=manifest_path, sumo_connection=sumo_connection)

        After initialization, files must be explicitly indexed into the EnsembleOnDisk object:

        >>> e.add_files(searchpath)

        When initialized, the ensemble can be uploaded to Sumo:

        >>> e.upload()

    Args:
        manifest_path (str): Absolute (full) path to the manifest file for the ensemble
        sumo_connection (fmu.sumo.SumoConnection): SumoConnection object


    """

    def __init__(self, manifest_path: str, sumo_connection):
        """
        manifest_path (str): Path to manifest for ensemble
        api (SumoConnection instance): Connection to Sumo.
        """

        self._manifest = self._load_manifest(manifest_path)
        self._fmu_ensemble_id = None
        self._files = []
        self.sumo_connection = sumo_connection
        self._sumo_parent_id = None
        self._on_sumo = None

    def __str__(self):
        s = f'{self.__class__}, {len(self._files)} files.'

        if self._sumo_parent_id is not None:
            s += f'\nInitialized on Sumo. Sumo_ID: {self._sumo_parent_id}'
        else:
            s += '\nNot initialized on Sumo.'

        s += '\nFMU casename: {}'.format(self.casename)

        return s

    def __repr__(self):
        return str(self.__str__)          

    @property
    def manifest(self):
        return self._manifest

    @property
    def casename(self):
        return self._manifest.get('fmu_ensemble').get('case')

    @property
    def sumo_parent_id(self):
        if self._sumo_parent_id is None:
            self._sumo_parent_id = self._get_sumo_parent_id()
        return self._sumo_parent_id

    @property
    def fmu_ensemble_id(self):
        if self._fmu_ensemble_id is None:
            self._fmu_ensemble_id = self._get_fmu_ensemble_id()
        return self._fmu_ensemble_id

    @property
    def files(self):
        return self._files

    def _load_manifest(self, manifest_path: str):
        """Given manifest path, load the yaml file from disk, return dict"""

        if not os.path.isfile(manifest_path):
            raise IOError('File does not exist: {}'.format(manifest_path))

        with open(manifest_path, 'r') as stream:
            yaml_data = yaml.safe_load(stream)

        return yaml_data

    def add_files(self, searchstring, formatting=True):
        """Add files to the ensemble, based on searchstring"""
        file_paths = self._find_file_paths(searchstring)

        for file_path in file_paths:
            file = FileOnDisk(path=file_path, formatting=formatting)

            if file.metadata:
                self._files.append(file)
            else:
                warnings.warn('No metadata, skipping file: {}'.format(file))

    def _find_file_paths(self, searchstring):
        """Given a searchstring, return yielded valid files as list
        of FileOnDisk instances"""
        files = [f for f in glob.glob(searchstring) if os.path.isfile(f)]
        if len(files) == 0:
            warnings.warn('No files found! Bad searchstring?')
            warnings.warn('Searchstring: {}'.format(searchstring))
        return files

    def _get_sumo_parent_id(self):
        """Call sumo, check if the ensemble is already there. Use fmu_ensemble_id for this."""

        # search for all ensembles on Sumo, matching on fmu_ensemble_id
        logging.info('this fmu_ensemble_id: {}'.format(self.fmu_ensemble_id))

        query = f'fmu_ensemble.fmu_ensemble_id:{self.fmu_ensemble_id}'
        search_results = self.sumo_connection.api.searchroot(query, select='source', buckets='source')

        try:
            hits = search_results.get('hits').get('hits')
        except AttributeError:
            if search_results.get('error').get('type') == 'index_not_found_exception':
                # index not found, crazy rare exception. Index is empty.
                sumo_parent_id = self._upload_manifest(self.manifest)
                logging.info('Ensemble registered. SumoID: {}'.format(sumo_parent_id))
                return sumo_parent_id

        except Exception as error:
            logging.debug('ERROR in hits. This is what the search results looked like:')
            logging.debug(search_results)
            raise error

        if len(hits) == 0:
            return None

        if len(hits) == 1:
            sumo_parent_id = hits[0].get('_id')
            logging.info(f'Already registered on Sumo with ID: {sumo_parent_id}')
            return sumo_parent_id

        raise Exception(f'Found {len(hits)} ensembles with the same ID on Sumo')

    def register(self):
        """
            Register this ensemble on Sumo. 
            Assumptions: If registering an already existing ensemble, it will be overwritten.
            ("register" might be a bad word for this...)

            Returns:
                sumo_parent_id (uuid4): Unique ID for this ensemble on Sumo
        """

        logging.info('Registering ensemble on Sumo')
        sumo_parent_id = self._upload_manifest(self.manifest)
        logging.info('Ensemble registered. SumoID: {}'.format(sumo_parent_id))
        self._sumo_parent_id = sumo_parent_id   # bad pattern, needs refactoring
        return sumo_parent_id

    def _upload_manifest(self, manifest:dict):
        """Given a manifest dict, upload it to Sumo"""
        response = self.sumo_connection.api.save_top_level_json(json=manifest)
        returned_object_id = response.json().get('objectid')
        return returned_object_id

    def _get_fmu_ensemble_id(self):
        """Look up and return ensemble_id from manifest"""
        fmu_ensemble_id = self.manifest.get('fmu_ensemble').get('fmu_ensemble_id')
        if not fmu_ensemble_id:
            raise ValueError('Could not get fmu_ensemble_id from ensemble metadata')
        return fmu_ensemble_id

    def _calculate_upload_stats(self, uploads:list, showplot:bool):
        """
        Given a list of results from file upload, calculate and return timing statistics
        for uploads
        """

        df = pd.DataFrame().from_dict(uploads)

        logging.info('_calculate_upload_stats, showplot is {}'.format(showplot))

        stats = {
            'blob': {'upload_time' : {'mean': df['blob_upload_time_elapsed'].mean(),
                                      'max': df['blob_upload_time_elapsed'].max(),
                                      'min': df['blob_upload_time_elapsed'].min(),
                                      'std': df['blob_upload_time_elapsed'].std(),
                                      },
                                },
            'metadata': {'upload_time' : {'mean': df['metadata_upload_time_elapsed'].mean(),
                                      'max': df['metadata_upload_time_elapsed'].max(),
                                      'min': df['metadata_upload_time_elapsed'].min(),
                                      'std': df['metadata_upload_time_elapsed'].std(),
                                      },
                                },
                        }

        if showplot:
            logging.info('plotting...')
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots()
            ax_sec = ax.twinx()

            df['blob_upload_time_elapsed'].plot(ax=ax, color='green', legend=True)
            df['blob_file_size'].plot(ax=ax_sec, linewidth=0, marker='.', color='blue', legend=True)
            df['metadata_upload_time_elapsed'].plot(ax=ax, color='black', legend=True)

            ax.set_ylabel('Time elapsed (seconds)')
            ax_sec.set_ylabel('File size (bytes)')

            ax.set_ylim(ymin=0)
            ax_sec.set_ylim(ymin=0)

            ax.legend(loc='upper left')
            ax_sec.legend(loc='upper right')

            plt.show()

        return stats

    def upload(self, threads=4, max_attempts=3, showplot=False, register_ensemble=False):
        """
        Trigger upload of files in this ensemble.

            Get sumo_parent_id. If None, ensemble is not registered on Sumo. Must be registered first.

            Upload all indexed files. Collect the files that have been uploaded OK, the
            ones that have failed and the ones that have been rejected.

            Retry the failed uploads X times.

        """

        if self.sumo_parent_id is None:
            logging.info('Ensemble is not registered on Sumo')
            if register_ensemble:
                logging.info('Registering ensemble')
                self.register()
                logging.info('Ensemble registered with ID: {}'.format(self.sumo_parent_id))

        if not self.files:
            warnings.warn('No files to upload. Check searchstring.')
            return

        _files_to_upload = [f for f in self.files]
        ok_uploads = []
        rejected_uploads = []

        _t0 = time.perf_counter()

        attempts = 0

        while _files_to_upload:
            upload_results = UPLOAD_FILES(files=_files_to_upload, sumo_parent_id=self.sumo_parent_id, sumo_connection=self.sumo_connection, threads=threads)

            ok_uploads += upload_results.get('ok_uploads') # append
            rejected_uploads += upload_results.get('rejected_uploads') # append
            failed_uploads = upload_results.get('failed_uploads') # replace

            # updating list of files for upload to only those who have failed
            _files_to_upload = [f.get('file') for f in failed_uploads]

            attempts += 1
            if attempts >= max_attempts:
                warnings.warn('Stopping after {} attempts'.format(attempts))
                break

            if not _files_to_upload:
                logging.info('No more files to upload, breaking the loop')
                break

            logging.info('sleep 3')
            time.sleep(3)
            logging.info('Retrying {} failed uploads after waiting 3 seconds'.format(len(failed_uploads)))

        _dt = time.perf_counter() - _t0

        # TODO: Should use timings from each file directly, rather than the total wall time
        if len(upload_results.get('ok_uploads')):
            _upload_statistics = self._calculate_upload_stats(ok_uploads, showplot=showplot)

            logging.info(_upload_statistics)

        logging.info(f"Total: {len(self.files)}" \
              f"\nOK: {len(upload_results.get('ok_uploads'))}" \
              f"\nFailures: {len(upload_results.get('failed_uploads'))}" \
              f"\nRejected: {len(upload_results.get('rejected_uploads'))}" \
              f"\nWall time: {_dt} seconds" \
                )

        if failed_uploads:
            logging.info('='*50)
            logging.info(f'{len(failed_uploads)} failed')
            logging.info('='*50)
            logging.info(failed_uploads)

        if len(rejected_uploads):
            logging.info(f'\n\n{len(rejected_uploads)} files rejected by Sumo. First 5 rejected files:')

            for u in rejected_uploads[0:4]:
                logging.info('\n'+'='*50)
                logging.info(u)

                logging.info(f"Filepath: {u.get('blob_file_path')}")
                logging.info(f"Metadata: [{u.get('metadata_upload_response_status_code')}] {u.get('metadata_upload_response_text')}")
                logging.info(f"Blob: [{u.get('blob_upload_response_status_code')}] {u.get('blob_upload_response_status_text')}")

                logging.info('-'*50+'\n')