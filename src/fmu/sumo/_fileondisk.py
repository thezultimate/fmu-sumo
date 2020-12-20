import yaml
import os
import datetime
import time
import logging

from fmu import sumo


class FileOnDisk:
    def __init__(self, path: str, metadata_path=None, formatting=True):
        """
        path (str): Path to file
        metadata_path (str): Path to metadata file. If not provided, 
                             path will be derived from file path.
        """

        if metadata_path is None:
            self.metadata_path = self.path_to_yaml_path(path) if formatting else path
        else:
            self.metadata_path = metadata_path

        self.path = os.path.abspath(path)
        self._metadata = self.get_metadata(self.metadata_path)
        self._bytestring = self.file_to_bytestring(path)
        self._size = None
        self._casename = None
        self._sumo_parent_id = None
        self._sumo_child_id = None
        self._sumo_child_id_blob = None
        self._filepath_relative_to_case_root = None
        self._basename = None
        self._dirname = None
        self._dtype = None
        self._fformat = None

        if self._metadata:
            self._metadata['datetime'] = self._datetime_now()
            #self._metadata['id'] = self._id_block()
            self._metadata['data']['relative_file_path'] = self.filepath_relative_to_case_root

    def __repr__(self):
        if not self.metadata:
            return f'\n# {self.__class__} \n# No metadata'

        s =  f'\n# {self.__class__}'
        s += f'\n# Diskpath: {self.path}'
        s += f'\n# Basename: {self.basename}'
        s += f'\n# Casename: {self.casename}'
        s += f'\n# Relative path: {self.filepath_relative_to_case_root}'
        s += f'\n# Bytestring length: {len(self.bytestring)}'
        s += f'\n# Data type: {self.dtype}'
        s += f'\n# File format: {self.fformat}'

        if self.sumo_child_id is None:
            s += '\n# Not uploaded to Sumo'
        else:
            s += f'\n# Uploaded to Sumo. Sumo_ID: {self.sumo_child_id}'

        s += '\n\n'

        return s

    @property
    def sumo_parent_id(self):
        return self._sumo_parent_id

    @property
    def sumo_child_id(self):
        return self._sumo_child_id

    @property
    def filepath_relative_to_case_root(self):
        if self._filepath_relative_to_case_root is None:
            self._filepath_relative_to_case_root = self._get_filepath_relative_to_case_root()
        return self._filepath_relative_to_case_root

    @property
    def size(self):
        """Size of the file"""
        if self._size is None:
            self._size = self._calculate_size(self.path)
        return self._size

    @property
    def casename(self):
        if self._casename is None:
            self._casename = self._get_casename()
        return self._casename

    @property
    def basename(self):
        if not self._basename:
            self._basename = os.path.basename(self.path)
        return self._basename

    @property
    def dirname(self):
        if not self._dirname:
            self._dirname = os.path.dirname(self.path)
        return self._dirname

    @property
    def dtype(self):
        if not self._dtype:
            self._dtype = self._get_dtype()
        return self._dtype

    @property
    def fformat(self):
        if not self._fformat:
            self._fformat = self._get_fformat()
        return self._fformat

    @property
    def metadata(self):
        return self._metadata

    @property
    def bytestring(self):
        return self._bytestring

    def _calculate_size(self, path):
        """calculate file size in bytes from path, return as int"""
        return os.path.getsize(path)

    def _datetime_now(self):
        """Return datetime now on FMU standard format"""
        return datetime.datetime.now().isoformat()

    def _get_filepath_relative_to_case_root(self):
        """Derive the local filepath from the absolute path"""
        casename = self._get_casename()

        logging.info(self.path)

        if casename not in self.path:
            raise IOError(f'Could not find casename ({casename}) in filepath: {self.path}')
        return self.path.split(casename)[-1][1:]

    def _get_casename(self):
        """Look up casename from metadata"""
        if not self.metadata:
            return 'No Metadata'

        case_name = self.metadata.get('fmu_ensemble').get('case')

        if not case_name:
            raise AttributeError('Could not get casename from metadata')

        return case_name

    def _get_dtype(self):
        """Look up file format from metadata"""

        dtype = self.metadata.get('class', {}).get('type')

        if dtype is None:
            logging.error('Could not get file format from metadata')
            logging.error('File: {}'.format(self.path))
            logging.error('Metadata file: {}'.format(self.metadata_path))
            raise AttributeError('Could not get file format')

        return dtype

    def _get_fformat(self):
        """Look up file format from metadata"""

        fformat = self.metadata.get('data', {}).get('format')

        if fformat is None:
            logging.error('Could not get file format from metadata')
            logging.error('File: {}'.format(self.path))
            logging.error('Metadata file: {}'.format(self.metadata_path))
            raise AttributeError('Could not get file format')

        return fformat

    def path_to_yaml_path(self, path):
        """
        Given a path, return the corresponding yaml file path
        according to FMU standards.
        /my/path/file.txt --> /my/path/.file.txt.yaml
        """

        dirname = os.path.dirname(path)
        basename = os.path.basename(path)

        return os.path.join(dirname, f'.{basename}.yaml')

    def get_metadata(self, metadata_path):
        try:
            return self.parse_yaml(metadata_path)
        except IOError:
            return None

    def parse_yaml(self, path):
        if not os.path.isfile(path):
            raise IOError(f'File does not exist: {path}')
        with open(path, 'r') as stream:
            data = yaml.safe_load(stream)
        return data

    def file_to_bytestring(self, path):
        """
        Given an path to a file, read as bytes,
        return bytestring. 
        """

        with open(path, 'rb') as f:
            bytestring = f.read()
        return bytestring

    def _upload_metadata(self, sumo_connection, sumo_parent_id):
        response = sumo_connection.api.save_child_level_json(json=self.metadata, parent_id=sumo_parent_id)
        return response

    def _upload_bytestring(self, sumo_connection, object_id, blob_url):
        response = sumo_connection.api.save_blob(blob=self.bytestring, object_id=object_id, url=blob_url)
        return response

    def upload_to_sumo(self, sumo_parent_id, sumo_connection):
        """Upload this file to Sumo"""

        if not sumo_parent_id:
            raise ValueError(f'status: failed, response: Failed, sumo_parent_id passed to upload_to_sumo: {sumo_parent_id}')

        # UPLOAD JSON
        _t0 = time.perf_counter()
        _t0_metadata = time.perf_counter()

        result = {}

        try:
            response = self._upload_metadata(sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id)

            _t1_metadata = time.perf_counter()
            result['metadata_upload_response_status_code'] = response.status_code
            result['metadata_upload_response_text'] = response.text
            result['metadata_upload_time_start'] = _t0_metadata
            result['metadata_upload_time_end'] = _t1_metadata
            result['metadata_upload_time_elapsed'] = _t1_metadata-_t0_metadata
            result['metadata_file_path'] = self.metadata_path
            result['metadata_file_size'] = self._calculate_size(self.metadata_path)

            # want these included even if returning before blob upload
            result['blob_file_path'] = self.path
            result['blob_file_size'] = self.size
        except sumo.TransientError as err:
            result['status'] = 'failed'
            result['metadata_upload_response_status_code'] = err.code
            result['metadata_upload_response_text'] = err.message
            return result
        except sumo.AuthenticationError as err:
            result['status'] = 'rejected'
            result['metadata_upload_response_status_code'] = err.code
            result['metadata_upload_response_text'] = err.message
            return result
        except sumo.PermanentError as err:
            result['status'] = 'rejected'
            result['metadata_upload_response_status_code'] = err.code
            result['metadata_upload_response_text'] = err.message
            return result

        self._sumo_child_id = response.json().get('objectid')
        blob_url = response.json().get('blob_url')

        # UPLOAD BLOB
        _t0_blob = time.perf_counter()
        response = self._upload_bytestring(sumo_connection=sumo_connection,
                                           object_id=self._sumo_child_id, blob_url=blob_url)
        _t1_blob = time.perf_counter()

        result['blob_upload_response_status_code'] = response.status_code
        result['blob_upload_response_text'] = response.text
        result['blob_upload_time_start'] = _t0_blob
        result['blob_upload_time_end'] = _t1_blob
        result['blob_upload_time_elapsed'] = _t1_blob-_t0_blob

        if response.status_code not in [200,201]:
            logging.info(response)
            result['status'] = 'failed'

        result['status'] = 'ok'

        return result