import sys
import pytest

try:
    sys.path.index('../src')
except ValueError:
    sys.path.append('../src')

from fmu.sumo import uploader


def test_uploader():
    #raise NotImplementedError('Must be refactored for Drogon')

    ensemble_metadata_path = 'data/test_ensemble_070/ensemble.yaml'
    env = 'fmu'
    threads = 1

    sumo_connection = uploader.SumoConnection(env=env)
    e = uploader.EnsembleOnDisk(ensemble_metadata_path=ensemble_metadata_path, sumo_connection=sumo_connection)
    with pytest.warns(UserWarning) as warnings_record:  # testdata contains one file with missing metadata
        e.add_files('data/test_ensemble_070/*.bin')

    with pytest.raises(IOError):
        # assert that uploading withouth registering fails
        e.upload(threads=threads)

    e.register()

    # Assert Ensemble is on Sumo
    query = f'fmu.ensemble.id:{e.fmu_ensemble_id}'
    search_results = sumo_connection.api.searchroot(query, select="source", buckets="source")
    hits = search_results.get('hits').get('hits')
    assert len(hits) == 1

    e.upload(threads=threads)

    # Assert that expected warning was given
    for w in warnings_record:
        print(w.message.args[0])
    assert len(warnings_record) == 1
    assert warnings_record[0].message.args[0].endswith("No metadata, skipping file.")

    # Assert children is on Sumo
    search_results = sumo_connection.api.search(query=f'{e.fmu_ensemble_id}')
    total = search_results.get('hits').get('total').get('value')
    print(search_results)
    assert total == 3
