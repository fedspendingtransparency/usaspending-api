import boto
import logging
import math
import mimetypes
import multiprocessing
import os

from django.conf import settings
from filechunkio import FileChunkIO
from rest_framework.exceptions import ParseError

from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger('console')


def check_types_and_assign_defaults(request_dict, defaults_dict):
    for field in defaults_dict.keys():
        request_dict[field] = request_dict.get(field, defaults_dict[field])
        # Validate the field's data type
        if not isinstance(request_dict[field], type(defaults_dict[field])):
            type_name = type(defaults_dict[field]).__name__
            raise InvalidParameterException('{} parameter not provided as a {}'.format(field, type_name))
        # Remove empty filters
        if request_dict[field] == defaults_dict[field]:
            del(request_dict[field])


def parse_limit(json_request):
    """Extracts the `limit` from a request and validates"""
    limit = json_request.get('limit')
    if limit:
        try:
            limit = int(json_request['limit'])
        except (ValueError, TypeError):
            raise ParseError('limit must be integer; {} given'.format(limit))
        if limit > settings.MAX_DOWNLOAD_LIMIT:
            msg = 'Requested limit {} beyond max supported ({})'
            raise ParseError(msg.format(limit, settings.MAX_DOWNLOAD_LIMIT))
    else:
        limit = settings.MAX_DOWNLOAD_LIMIT
    return limit   # None is a workable slice argument


def verify_requested_columns_available(sources, requested):
    """Ensures the user-requested columns are availble to write to"""
    bad_cols = set(requested)
    for source in sources:
        bad_cols -= set(source.columns(requested))
    if bad_cols:
        raise InvalidParameterException('Unknown columns: {}'.format(bad_cols))


# Multipart upload functions copied from Fabian Topfstedt's solution
# http://www.topfstedt.de/python-parallel-s3-multipart-upload-with-retries.html
def multipart_upload(bucketname, regionname, source_path, keyname, acl='private', headers={}, guess_mimetype=True,
                     parallel_processes=4):
    """Parallel multipart upload."""
    bucket = boto.s3.connect_to_region(regionname).get_bucket(bucketname)
    if guess_mimetype:
        mtype = mimetypes.guess_type(keyname)[0] or 'application/octet-stream'
        headers.update({'Content-Type': mtype})

    mp = bucket.initiate_multipart_upload(keyname, headers=headers)

    source_size = os.stat(source_path).st_size
    bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)),
                          5242880)
    chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))

    pool = multiprocessing.Pool(processes=parallel_processes)
    for i in range(chunk_amount):
        offset = i * bytes_per_chunk
        remaining_bytes = source_size - offset
        bytes = min([bytes_per_chunk, remaining_bytes])
        part_num = i + 1
        pool.apply_async(_upload_part, [bucketname, regionname, mp.id,
                         part_num, source_path, offset, bytes])
    pool.close()
    pool.join()

    if len(mp.get_all_parts()) == chunk_amount:
        mp.complete_upload()
        key = bucket.get_key(keyname)
        key.set_acl(acl)
    else:
        mp.cancel_upload()


def _upload_part(bucketname, regionname, multipart_id, part_num, source_path, offset, bytes, amount_of_retries=10):
    """Uploads a part with retries."""
    bucket = boto.s3.connect_to_region(regionname).get_bucket(bucketname)

    def _upload(retries_left=amount_of_retries):
        try:
            logging.info('Start uploading part #%d ...' % part_num)
            for mp in bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    with FileChunkIO(source_path, 'r', offset=offset,
                                     bytes=bytes) as fp:
                        mp.upload_part_from_file(fp=fp, part_num=part_num)
                    break
        except Exception as exc:
            if retries_left:
                _upload(retries_left=retries_left - 1)
            else:
                logging.info('... Failed uploading part #%d' % part_num)
                raise exc
        else:
            logging.info('... Uploaded part #%d' % part_num)
    _upload()


def write_to_download_log(message, download_job=None, is_debug=False, is_error=False, other_params={}):
    log_dict = {
        'message': message,
        'message_type': 'USAspendingDownloader'
    }

    if download_job:
        log_dict['download_job_id'] = download_job.download_job_id
        log_dict['file_name'] = download_job.file_name
        log_dict['json_request'] = download_job.json_request
        if download_job.error_message:
            log_dict['error_message'] = download_job.error_message

    for param in other_params:
        if param not in log_dict:
            log_dict[param] = other_params[param]

    if is_error:
        logger.error(log_dict)
    elif is_debug:
        logger.debug(log_dict)
    else:
        logger.info(log_dict)
