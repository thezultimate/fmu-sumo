import sys

try:
    sys.path.index('../src')
except ValueError:
    sys.path.append('../src')

from fmu import sumo


def test_uploader():
    manifest_path = 'data/MyCaseName2/fmu_ensemble.yaml'
    search_path = 'data/MyCaseName2/'
    env = 'dev'
    threads = 1

    sumo_connection = sumo.SumoConnection(env=env)
    e = sumo.EnsembleOnDisk(manifest_path=manifest_path, sumo_connection=sumo_connection)
    e.add_files(search_path + '/*.bin')
    e.upload(threads=threads)

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
