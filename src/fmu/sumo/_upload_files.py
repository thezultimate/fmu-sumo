import logging

def UPLOAD_FILES(files: list, sumo_parent_id: str, sumo_connection, threads=4):
    """
    Upload files, including JSON/manifest, to specified ensemble

    files: list of FileOnDisk objects
    sumo_parent_id: sumo_parent_id for the parent ensemble

    Upload is kept outside classes to use multithreading.
    """
    
    import time
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor

    def _upload_files(files, sumo_connection, sumo_parent_id, threads=threads):
        with ThreadPoolExecutor(threads) as executor:
            results = executor.map(_upload_file, [(file, sumo_connection, sumo_parent_id) for file in files])
        return results

    def _upload_file(arg):
        file, sumo_connection, sumo_parent_id = arg
        if not file:
            return
        result = file.upload_to_sumo(sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id)
        return result

    logging.info('*'*35)
    logging.info(f'{datetime.isoformat(datetime.now())}: Uploading {len(files)} files with {threads} threads on environment {sumo_connection.env}')

    results = _upload_files(files=files, sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id, threads=threads)

    ok_uploads = []
    failed_uploads = []
    rejected_uploads = []

    for r in results:
        _status = r.get('status')
        if not _status:
            raise ValueError('File upload result returned with no "status" attribute')
        if _status == 'ok':
            ok_uploads.append(r)
        elif _status == 'rejected':
            rejected_uploads.append(r)
        else:
            failed_uploads.append(r)

    if not (len(ok_uploads)+len(failed_uploads)+len(rejected_uploads) == len(files)):
        raise ValueError(f'Sum of ok_uploads ({len(ok_uploads)}, rejected_uploads ({len(rejected_uploads)}) ' \
                         f'and failed_uploads ({len(failed_uploads)}) does not add up to ' \
                         f'number of files total ({len(files)})' \
                         )

    return {'ok_uploads': ok_uploads, 'failed_uploads': failed_uploads, 'rejected_uploads': rejected_uploads}


