import sys
import pytest

try:
    sys.path.index('../src')
except ValueError:
    sys.path.append('../src')

from fmu.sumo import uploader


def test_uploader():
    manifest_path = 'data/MyCaseName2/fmu_ensemble.yaml'
    search_path = 'data/MyCaseName2/'
    env = 'dev'
    threads = 1

    sumo_connection = uploader.SumoConnection(env=env)
    e = uploader.EnsembleOnDisk(manifest_path=manifest_path, sumo_connection=sumo_connection)
    with pytest.warns(UserWarning) as warnings_record:  # testdata contains one file with missing metadata
        e.add_files(search_path + '/*.bin')
    e.upload(threads=threads)

    # Assert that expected warning was given
    for w in warnings_record:
        print(w.message.args[0])
    assert len(warnings_record) == 1
    assert warnings_record[0].message.args[0].endswith("No metadata, skipping file.")

    # Assert Ensemble is on Sumo
    query = f'fmu_ensemble.fmu_ensemble_id:{e.fmu_ensemble_id}'
    search_results = sumo_connection.api.searchroot(query, select="source", buckets="source")
    hits = search_results.get('hits').get('hits')
    assert len(hits) == 1

    # Assert children is on Sumo
    search_results = sumo_connection.api.search(query=f'{e.fmu_ensemble_id}')
    total = search_results.get('hits').get('total').get('value')
    print(search_results)
    assert total == 3
