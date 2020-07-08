
def UPLOAD_FILES(files:list, sumo_parent_id:str, sumo_connection, threads=4):
    """
    Upload files, including JSON/manifest, to specified ensemble

    files: list of FileOnDisk objects
    sumo_parent_id: sumo_parent_id for the parent ensemble

    Upload is kept outside classes to use multithreading.
    """
    
    import time
    from concurrent.futures import ThreadPoolExecutor

    def _upload_files(files, sumo_connection, sumo_parent_id, threads=4):
        with ThreadPoolExecutor(threads) as executor:
            results = executor.map(_upload_file, [(file, sumo_connection, sumo_parent_id) for file in files])
        return results

    def _upload_file(arg):
        file, sumo_connection, sumo_parent_id = arg
        result = file.upload_to_sumo(sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id)
        result['file'] = file
        return result

    def _print_upload_result(result):
        file = result.get('file')
        response = result.get('response')

        json_status_code = result.get('response').get('metadata').status_code
        json_response_text = result.get('response').get('metadata').text

        if result.get('response').get('blob'):
            blob_status_code = result.get('response').get('blob').status_code
        else:
            blob_status_code = None

        if result.get('response').get('blob'):
            blob_response_text = result.get('response').get('blob').text
        else:
            blob_response_text = None


        print(f"{file.filepath_relative_to_case_root}:")
        print(f"JSON: [{json_status_code}] {json_response_text}")
        print(f"Blob: [{blob_status_code}] {blob_response_text}\n")

    def _print_rejected_uploads(rejected_uploads):

        print(f'\n\n{len(rejected_uploads)} files rejected by Sumo:')

        for u in rejected_uploads:
            print('\n'+'-'*50)

            print(f"File: {u['file'].filepath_relative_to_case_root}")
            metadata_response = u.get('response').get('metadata')
            blob_response = u.get('response').get('blob')
            print(f"Metadata: [{metadata_response.status_code}] {metadata_response.text}")

            if blob_response:
                print(f"Blob: [{blob_response.status_code}] {ublob_response.text}")

            print('-'*50+'\n')



    def _summary(ok_uploads, to_file='upload_log.txt'):
        #print('Total time spent: {} seconds'.format(_dt))

        lines = []    
        for result in ok_uploads:
            filepath = result.get('file').filepath_relative_to_case_root
            time_elapsed = result.get('timing', {}).get('total', {}).get('time_elapsed')
            if time_elapsed:
                time_elapsed = round(time_elapsed, 2)
            time_start_metadata = round(result.get('timing', {}).get('metadata', {}).get('time_start'))
            time_start_blob = round(result.get('timing', {}).get('blob', {}).get('time_start'))
            statuscodeblob = result.get('response').get('blob').status_code
            statuscodemetadata = result.get('response').get('metadata').status_code
            #line = f'{filepath}: {time_elapsed} s. Metadata start: {time_start_metadata}, blob start: {time_start_blob}. (Metadata: {statuscodemetadata}, blob: {statuscodeblob})\n'
            line = f'{filepath}: {time_elapsed} s. Metadata: {statuscodemetadata}, blob: {statuscodeblob}\n'
            lines.append(line)
        if not to_file:
            for line in lines:
                print(line)
        else:
            with open(to_file, 'w') as f:
                for line in lines:
                    f.write(line)


    print(f'UPLOADING {len(files)} files with {threads} threads.')
    print(f'Environment is {sumo_connection.env}')

    results = _upload_files(files=files, sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id, threads=threads)

    ok_uploads = []
    failed_uploads = []
    rejected_uploads = []

    for r in results:
        _status = r.get('status')
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










    if not failed_uploads:
        _t1 = time.perf_counter()
        _dt = _t1-_t0

        print('\n==== UPLOAD DONE ====')
        _summary(ok_uploads)
        if rejected_uploads:
            _print_rejected_uploads(rejected_uploads)

        return {'elements' : [s.basename for s in files],
                'sumo_parent_id' : sumo_parent_id,
                'count_total' : len(files),
                'count_rejected': len(rejected_uploads),
                'count_failed': len(failed_uploads),
                'count_ok': len(ok_uploads),
                'time_start' : _t0,
                'time_end' : _t1,
                'time_elapsed' : _dt,}

    if not ok_uploads:
        print('\nALL FILES FAILED')
        print('\nNot trying again.')
        return {'elements' : [s.basename for s in files],
                'sumo_parent_id' : sumo_parent_id,
                'count' : len(files),
                'count_total' : len(files),
                'count_rejected': len(rejected_uploads),
                'count_failed': len(failed_uploads),
                'count_ok': len(ok_uploads),
                'time_start' : _t0,
                'time_end' : _t1,
                'time_elapsed' : _dt,}
    else:
        if failed_uploads:
            print('\nSome uploads failed:')

    for result in failed_uploads:
        _print_upload_result(result)

    # second attempt
    print('\nRetrying {} failed uploads.'.format(len(failed_uploads)))
    failed_files = [result.get('file') for result in failed_uploads]
    results = _upload_files(files=failed_files, sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id, threads=threads)
    _t1 = time.perf_counter()
    _dt = _t1-_t0

    failed_uploads = []

    for r in results:
        _status = r.get('status')
        if _status == 'ok':
            ok_uploads.append(r)
        elif _status == 'rejected':
            rejected_uploads.append(r)
        else:
            failed_uploads.append(r)

    if failed_uploads:
        print('Uploads still failed after second attempt:')
        for result in failed_uploads:
            _print_upload_result(result)
    else:
        print('After second attempt, all uploads are OK. Summary:')
        _summary(ok_uploads)

        return {'elements' : [s.basename for s in files],
                'sumo_parent_id' : sumo_parent_id,
                'count' : len(files),
                'count_total' : len(files),
                'count_rejected': len(rejected_uploads),
                'count_failed': len(failed_uploads),
                'count_ok': len(ok_uploads),
                'time_start' : _t0,
                'time_end' : _t1,
                'time_elapsed' : _dt,}

    # third attemp
    print('\nRetrying {} failed uploads.'.format(len(failed_uploads)))
    failed_files = [result.get('file') for result in failed_uploads]
    results = _upload_files(files=failed_files, sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id, threads=threads)
    _t1 = time.perf_counter()
    _dt = _t1-_t0

    failed_uploads = []
    for r in results:
        _status = r.get('status')
        if _status == 'ok':
            ok_uploads.append(r)
        elif _status == 'rejected':
            rejected_uploads.append(r)
        else:
            failed_uploads.append(r)

    if failed_uploads:
        print('{} uploads still failed after third attempt:'.format(len(failed_uploads)))
        for result in failed_uploads:
            _print_upload_result(result)

        print('\n\n=========== NO MORE ATTEMPTS MADE =========')
    else:
        print('After third attempt, all uploads are OK. Summary:')
        _summary(ok_uploads)

    return {'elements' : [s.basename for s in files],
            'sumo_parent_id' : sumo_parent_id,
            'count' : len(files),
            'count_total' : len(files),
            'count_rejected': len(rejected_uploads),
            'count_failed': len(failed_uploads),
            'count_ok': len(ok_uploads),
            'time_start' : _t0,
            'time_end' : _t1,
            'time_elapsed' : _dt,}