import os
import sys
import pytest
import time
from pathlib import Path

from fmu.sumo import uploader

# run the tests from the root dir
TEST_DIR = Path(__file__).parent / "../"
os.chdir(TEST_DIR)

ENV = "fmu"


class SumoConnection:
    def __init__(self, env):
        self.env = env
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = uploader.SumoConnection(env=self.env)
        return self._connection


def test_initialisation():
    sumo_connection = SumoConnection(env=ENV).connection
    e = uploader.EnsembleOnDisk(ensemble_metadata_path="tests/data/test_ensemble_070/ensemble.yaml",
                                sumo_connection=sumo_connection)


def test_upload_without_registration():
    sumo_connection = uploader.SumoConnection(env=ENV)
    e = uploader.EnsembleOnDisk(ensemble_metadata_path="tests/data/test_ensemble_070/ensemble.yaml",
                                sumo_connection=sumo_connection)
    with pytest.raises(IOError):
        # assert that uploading withouth registering fails
        e.upload(threads=1)


def test_ensemble():
    """
        Upload ensemble to Sumo. Assert that the ensemble is there and that only one
        ensemble with this ID exists.
    """
    sumo_connection = uploader.SumoConnection(env=ENV)
    e = uploader.EnsembleOnDisk(ensemble_metadata_path="tests/data/test_ensemble_070/ensemble.yaml",
                                sumo_connection=sumo_connection)
    query = f'fmu.ensemble.id:{e.fmu_ensemble_id}'

    # assert that it is not there in the first place
    search_results = sumo_connection.api.searchroot(query, select="source", buckets="source")
    hits = search_results.get('hits').get('hits')
    assert len(hits) == 0

    # register it
    e.register()

    # assert that it is there now
    time.sleep(3)  # wait 3 seconds
    search_results = sumo_connection.api.searchroot(query, select="source", buckets="source")
    hits = search_results.get('hits').get('hits')
    assert len(hits) == 1


def test_one_file():
    """
        Upload one file to Sumo. Assert that it is there.
    """
    sumo_connection = uploader.SumoConnection(env=ENV)
    e = uploader.EnsembleOnDisk(ensemble_metadata_path="tests/data/test_ensemble_070/ensemble.yaml",
                                sumo_connection=sumo_connection)
    e.register()
    e.add_files('tests/data/test_ensemble_070/surface.bin')

    # Assert children is on Sumo
    search_results = sumo_connection.api.search(query=f'{e.fmu_ensemble_id}')
    total = search_results.get('hits').get('total').get('value')
    assert total == 1


def test_missing_metadata():
    """
        Try to upload files where one does not have metadata. Assert that warning is given
        and that upload commences with the other files. Check that the children are present.
    """
    sumo_connection = uploader.SumoConnection(env=ENV)
    e = uploader.EnsembleOnDisk(ensemble_metadata_path="tests/data/test_ensemble_070/ensemble.yaml",
                                sumo_connection=sumo_connection)

    # Assert that expected warning was given
    with pytest.warns(UserWarning) as warnings_record:  # testdata contains one file with missing metadata
        e.add_files('tests/data/test_ensemble_070/*.bin')
        for _ in warnings_record:
            assert len(warnings_record) == 1, warnings_record
            assert warnings_record[0].message.args[0].endswith("No metadata, skipping file.")

    # Assert children is on Sumo
    search_results = sumo_connection.api.search(query=f'{e.fmu_ensemble_id}')
    total = search_results.get('hits').get('total').get('value')
    assert total == 1
