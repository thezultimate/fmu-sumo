def UPLOAD_FILES(files:list, sumo_parent_id:str, api=None, threads=4):
    """
    Upload files, including JSON/manifest, to specified ensemble

    files: list of FileOnDisk objects
    sumo_parent_id: sumo_parent_id for the parent ensemble

    Upload is kept outside classes to use multithreading.
    """

    def _upload_files(files, api, sumo_parent_id, threads=4):
        with ThreadPoolExecutor(threads) as executor:
            results = executor.map(_upload_file, [(file, api, sumo_parent_id) for file in files])
        return results

    def _upload_file(arg):
        file, api, sumo_parent_id = arg
        result = file.upload_to_sumo(api=api, sumo_parent_id=sumo_parent_id)
        result['file'] = file
        return result

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


    _t0 = time.perf_counter()

    print(f'UPLOADING {len(files)} files with {threads} threads.')

    # first attempt
    results = _upload_files(files=files, api=api, sumo_parent_id=sumo_parent_id, threads=threads)

    ok_uploads = []
    failed_uploads = []
    for r in results:
        _status = r.get('status')
        if _status == 'ok':
            ok_uploads.append(r)
        else:
            failed_uploads.append(r)

    if not failed_uploads:
        _t1 = time.perf_counter()
        _dt = _t1-_t0

        print('\n==== UPLOAD DONE ====')
        _summary(ok_uploads)
        return {'elements' : [s.basename for s in files],
                'sumo_parent_id' : sumo_parent_id,
                'count' : len(files),
                'time_start' : _t0,
                'time_end' : _t1,
                'time_elapsed' : _dt,}

    if not ok_uploads:
        print('\nALL FILES FAILED')
    else:
        if failed_uploads:
            print('\nSome uploads failed:')

    for result in failed_uploads:
        file = result.get('file')
        response = result.get('response')
        print(f'{file.filepath_relative_to_case_root}: {response}')

    print('\nRetrying {} failed uploads.'.format(len(failed_uploads)))
    failed_files = [result.get('file') for result in failed_uploads]
    results = _upload_files(files=failed_files, api=api, sumo_parent_id=sumo_parent_id, threads=threads)
    _t1 = time.perf_counter()
    _dt = _t1-_t0

    failed_uploads = []
    for r in results:
        _status = r.get('status')
        if _status == 'ok':
            ok_uploads.append(r)
        else:
            failed_uploads.append(r)

    if failed_uploads:
        print('Uploads still failed after second attempt:')
        for result in failed_uploads:
            file = result.get('file')
            response = result.get('response')
            print(f'{file.filepath_relative_to_case_root}: {response}')
    else:
        print('After second attempt, all uploads are OK. Summary:')
        _summary(ok_uploads)

        return {'elements' : [s.basename for s in files],
                'sumo_parent_id' : sumo_parent_id,
                'count' : len(files),
                'time_start' : _t0,
                'time_end' : _t1,
                'time_elapsed' : _dt,}

    print('\nRetrying {} failed uploads.'.format(len(failed_uploads)))
    failed_files = [result.get('file') for result in failed_uploads]
    results = _upload_files(files=failed_files, api=api, sumo_parent_id=sumo_parent_id, threads=threads)
    _t1 = time.perf_counter()
    _dt = _t1-_t0

    failed_uploads = []
    for r in results:
        _status = r.get('status')
        if _status == 'ok':
            ok_uploads.append(r)
        else:
            failed_uploads.append(r)

    if failed_uploads:
        print('{} uploads still failed after third attempt:'.format(len(failed_uploads)))
        for result in failed_uploads:
            file = result.get('file')
            response = result.get('response')
            print(f'{file.filepath_relative_to_case_root}: {response}')

        print('\n\n=========== NO MORE ATTEMPTS MADE =========')
    else:
        print('After second attempt, all uploads are OK. Summary:')
        _summary(ok_uploads)

    return {'elements' : [s.basename for s in files],
            'sumo_parent_id' : sumo_parent_id,
            'count' : len(files),
            'time_start' : _t0,
            'time_end' : _t1,
            'time_elapsed' : _dt,}