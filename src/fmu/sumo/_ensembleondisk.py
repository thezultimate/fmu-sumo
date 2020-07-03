import os
import glob
import yaml
from fmu.sumo._fileondisk import FileOnDisk
from fmu.sumo._connection import SumoConnection
from fmu.sumo._upload_files import UPLOAD_FILES

class EnsembleOnDisk:
    """
    Class to hold information about an ERT run on disk.
    """

    def __init__(self, manifest_path:str, sumo_connection):
        """
        manifest_path (str): Path to manifest for ensemble
        api (SumoConnection instance): Connection to Sumo.
        """

        print('INIT EnsembleOnDisk')

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
    def on_sumo(self):
        if self._on_sumo is None:
            self.find_ensemble_on_sumo()
        return self._on_sumo            

    @property
    def manifest(self):
        return self._manifest

    @property
    def casename(self):
        return self._manifest.get('case')

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

    def find_ensemble_on_sumo(self):
        """Call Sumo, search for this ensemble. Return True if found, and set self._sumo_parent_id.
        return False if not.

        Criteria for ensemble identified on Sumo: fmu_ensemble_id
        """

        ensembles_on_sumo = [e for e in EnsemblesOnSumo(sumo_connection=self.sumo_connection).ensembles]

        for ensemble_on_sumo in ensembles_on_sumo:
            if self.fmu_ensemble_id == ensemble_on_sumo.fmu_ensemble_id:
                print('Found it on Sumo')
                self.sumo_parent_id = ensemble_on_sumo.sumo_parent_id
                return True
            print('Not found on Sumo')
            return False


    def add_files(self, searchstring):
        """Add files to the ensemble, based on searchstring"""
        file_paths = self._find_file_paths(searchstring)
        self._files = []
        for file_path in file_paths:
            file = FileOnDisk(path=file_path)
            if file.metadata:
                self._files.append(file)
            else:
                print('No metadata, skipping file: {}'.format(file))

    def _find_file_paths(self, searchstring):
        """Given a searchstring, return yielded valid files as list
        of FileOnDisk instances"""
        files = [f for f in glob.glob(searchstring) if os.path.isfile(f)]
        if len(files) == 0:
            print('No files found! Bad searchstring?')
            print('Searchstring: {}'.format(searchstring))
        return files

    def _get_sumo_parent_id(self):
        """Call sumo, check if the ensemble is already there. Use fmu_ensemble_id for this."""

        print('Getting SumoParentID')

        # search for all ensembles on Sumo, matching on fmu_ensemble_id
        print('this fmu_ensemble_id:')
        print(self.fmu_ensemble_id)

        query = f'fmu_ensemble.fmu_ensemble_id:{self.fmu_ensemble_id}'
        search_results = self.sumo_connection.api.searchroot(query, select='source', buckets='source')

        hits = search_results.get('hits').get('hits')

        if len(hits) == 0:
            print('No matching ensembles found on Sumo --> Not registered on Sumo')
            print('Registering ensemble on Sumo')
            sumo_parent_id = self._upload_manifest(self.manifest)
            print('Ensemble registered. SumoID: {}'.format(sumo_parent_id))
            return sumo_parent_id

        if len(hits) == 1:
            print('Found one matching ensemble on Sumo --> Registered on Sumo')
            return hits[0].get('_id')

        raise DuplicateSumoEnsemblesError(f'Found {len(hits)} ensembles with the same ID on Sumo')

    def _upload_manifest(self, manifest:dict):
        """Given a manifest dict, upload it to Sumo"""
        print('UPLOAD MANIFEST')
        response = self.sumo_connection.api.save_top_level_json(json=manifest)
        returned_object_id = response.text
        return returned_object_id

    def _load_manifest(self, manifest_path:str):
        """Given manifest path, load the yaml file, return dict"""

        if not os.path.isfile(manifest_path):
            raise IOError('File does not exist: {}'.format(manifest_path))

        with open(manifest_path, 'r') as stream:
            yaml_data = yaml.safe_load(stream)

        return yaml_data

    def _get_fmu_ensemble_id(self):
        """Look up and return ensemble_id from manifest"""
        fmu_ensemble_id = self.manifest.get('fmu_ensemble').get('fmu_ensemble_id')
        return fmu_ensemble_id

    def upload(self, threads=4):
        """Trigger upload of files in this ensemble"""
        if self._sumo_parent_id is None:
            self._sumo_parent_id = self._get_sumo_parent_id()

        upload_response = UPLOAD_FILES(files=self.files, sumo_parent_id=self._sumo_parent_id, sumo_connection=self.sumo_connection, threads=threads)
        print(f'Uploaded {len(self.files)} in {upload_response.get("time_elapsed")} seconds')

        print('upload done')