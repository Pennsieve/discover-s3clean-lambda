import os
import json

import boto3
from botocore.exceptions import ClientError

import structlog

# Configure JSON logs in a format that ELK can understand
# --------------------------------------------------
def rewrite_event_to_message(logger, name, event_dict):
    """
    Rewrite the default structlog `event` to a `message`.
    """
    event = event_dict.pop('event', None)
    if event is not None:
        event_dict['message'] = event
    return event_dict


def add_log_level(logger, name, event_dict):
    event_dict['log_level'] = name.upper()
    return event_dict


structlog.configure(
    processors=[
        rewrite_event_to_message,
        add_log_level,
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt='iso'),
        structlog.processors.JSONRenderer()])

# Main lambda handler
# --------------------------------------------------

ENVIRONMENT = os.environ['ENVIRONMENT']
SERVICE_NAME = os.environ['SERVICE_NAME']
TIER = os.environ['TIER']
FULL_SERVICE_NAME = f'{SERVICE_NAME}-{TIER}'

if ENVIRONMENT == 'local':
    S3_URL = 'http://localstack:4566'
else:
    S3_URL = None

S3_CLIENT = boto3.client('s3', endpoint_url=S3_URL)
PAGINATOR = S3_CLIENT.get_paginator('list_objects_v2')

S3DeleteMarkersTag = "DeleteMarkers"
S3VersionsTag = "Versions"
S3LastModifiedTag = "LastModified"
S3IsLatestTag = "IsLatest"
S3VersionIdTag = "VersionId"

CleanupStageInitial = "INITIAL"
CleanupStageFailure = "FAILURE"
CleanupStageUnpublish = "UNPUBLISH"

FileActionKey = "file-actions.json"

FileActionTag = "action"
FileActionBucketTag = "bucket"
FileActionPathTag = "path"
FileActionVersionTag = "versionId"

FileActionCopy = "CopyFile"
FileActionKeep = "KeepFile"
FileActionDelete = "DeleteFile"
FileActionUnknown = "Unknown"

NoValue = "(none)"

def lambda_handler(event, context, s3_client=S3_CLIENT, s3_paginator=PAGINATOR):
    # Create basic Pennsieve log context
    log = structlog.get_logger()
    log = log.bind(**{'class': f'{lambda_handler.__module__}.{lambda_handler.__name__}'})
    log = log.bind(pennsieve={'service_name': FULL_SERVICE_NAME})

    try:
        log.info('Reading environment')
        asset_bucket_id = os.environ['ASSET_BUCKET']
        assets_prefix = os.environ['DATASET_ASSETS_KEY_PREFIX']

        log.info('Parsing event')

        publish_bucket_id = event['publish_bucket']
        embargo_bucket_id = event['embargo_bucket']
        s3_key_prefix = event['s3_key_prefix']
        cleanup_stage = event.get("cleanup_stage", CleanupStageInitial)
        workflow_id = int(event.get("workflow_id", "4"))
        dataset_id = event.get("published_dataset_id","-1")
        dataset_version = event.get("published_dataset_version", "-1")

        if workflow_id == 5:
            purge_v5(log, asset_bucket_id, assets_prefix, publish_bucket_id, embargo_bucket_id, s3_key_prefix, s3_client, s3_paginator, cleanup_stage, dataset_id, dataset_version)
        else:
            purge_v4(log, asset_bucket_id, assets_prefix, publish_bucket_id, embargo_bucket_id, s3_key_prefix, s3_client, s3_paginator)

    except Exception as e:
        log.error(e, exc_info=True)
        raise

def purge_v4(log, asset_bucket_id, assets_prefix, publish_bucket_id, embargo_bucket_id, s3_key_prefix_evt, s3_client, s3_paginator):
    log.info(f"purge_v4() asset_bucket_id: {asset_bucket_id} assets_prefix: {assets_prefix} publish_bucket_id: {publish_bucket_id} embargo_bucket_id: {embargo_bucket_id} s3_key_prefix_evt: {s3_key_prefix_evt}")
    try:
        # Ensure the S3 key ends with a '/'
        if s3_key_prefix_evt.endswith('/'):
            s3_key_prefix = s3_key_prefix_evt
        else:
            s3_key_prefix = '{}/'.format(s3_key_prefix_evt)

        assert s3_key_prefix.endswith('/')
        assert len(s3_key_prefix) > 1  # At least one character + slash

        dataset_assets_prefix = '{}/{}'.format(assets_prefix, s3_key_prefix)

        log.info('Deleting objects from bucket {} under key {}'.format(publish_bucket_id, s3_key_prefix))
        delete(s3_client, s3_paginator, publish_bucket_id, s3_key_prefix, is_requester_pays=True)

        log.info('Deleting objects from bucket {} under key {}'.format(embargo_bucket_id, s3_key_prefix))
        delete(s3_client, s3_paginator, embargo_bucket_id, s3_key_prefix, is_requester_pays=True)

        log.info('Deleting objects from bucket {} under key {}'.format(asset_bucket_id, dataset_assets_prefix))
        delete(s3_client, s3_paginator, asset_bucket_id, dataset_assets_prefix)

    except Exception as e:
        log.error(e, exc_info=True)
        raise

def delete(s3_client, s3_paginator, bucket, prefix, is_requester_pays=False):
    requester_pays = {'RequestPayer': 'requester'} if is_requester_pays else {}

    pages = s3_paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={
        'PageSize': 1000
    }, **requester_pays)

    items_to_delete = dict(Objects=[])

    for page in pages:
        has_contents = page.get('Contents', None)
        if has_contents:
            for item in page['Contents']:
                items_to_delete['Objects'].append(dict(Key=item['Key']))
                # flush once aws limit reached
                if len(items_to_delete['Objects']) >= 1000:
                    s3_client.delete_objects(Bucket=bucket, Delete=items_to_delete, **requester_pays)
                    items_to_delete = dict(Objects=[])

            # flush the rest
            if len(items_to_delete['Objects']):
                s3_client.delete_objects(Bucket=bucket, Delete=items_to_delete, **requester_pays)

def purge_v5(log, asset_bucket_id, assets_prefix, publish_bucket_id, embargo_bucket_id, s3_client, s3_paginator, cleanup_stage, dataset_id, dataset_version):
    log.info(f"purge_v5() asset_bucket_id: {asset_bucket_id} assets_prefix: {assets_prefix} publish_bucket_id: {publish_bucket_id} embargo_bucket_id: {embargo_bucket_id} cleanup_stage: {cleanup_stage} dataset_id: {dataset_id} dataset_version: {dataset_version}")
    if cleanup_stage == CleanupStageInitial:
        # do nothing on initial cleanup during publishing
        log.info("purge_v5() CleanupStageInitial ~> nothing to do")
        return

    if cleanup_stage == CleanupStageUnpublish:
        log.info("purge_v5() CleanupStageUnpublish ~> will delete all versions of files")
        # Delete all versions of files in the Publish Bucket
        delete_all_versions(log, s3_client, publish_bucket_id, dataset_id)
        # Delete all versions of files in the Embargo Bucket
        delete_all_versions(log, s3_client, embargo_bucket_id, dataset_id)
        # Delete all files in the Public Assets Bucket
        dataset_assets_prefix = '{}/{}'.format(assets_prefix, dataset_id)
        delete(s3_client, s3_paginator, asset_bucket_id, dataset_assets_prefix)

    if cleanup_stage == CleanupStageFailure:
        # Undo File Actions in the Publish Bucket
        undo_actions(log, s3_client, publish_bucket_id, dataset_id)
        # Undo File Actions in the Embargo Bucket
        undo_actions(log, s3_client, embargo_bucket_id, dataset_id)
        # Clean up the Public Assets Bucket
        dataset_assets_prefix = '{}/{}/{}'.format(assets_prefix, dataset_id, dataset_version)
        delete(s3_client, s3_paginator, asset_bucket_id, dataset_assets_prefix)

def delete_all_versions(log, s3_client, bucket_id, dataset_id):
    log.info(f"delete_all_versions() bucket_id: {bucket_id} dataset_id: {dataset_id}")

    prefix = f"{dataset_id}/"
    paginator = s3_client.get_paginator('list_object_versions')
    pages = paginator.paginate(Bucket=bucket_id, Prefix=prefix, PaginationConfig={'PageSize': 1000})
    folder_list = []

    # delete all the files
    for page in pages:
        for o in page.get("DeleteMarkers", []) + page.get("Versions", []):
            key = o.get("Key","")
            folder = key[-1] == '/'
            if folder:
                # object is a folder, put it on a list to delete at the end
                folder_list.append(o)
            else:
                # delete this object version
                version = o.get("VersionId")
                delete_object_version(s3_client, bucket_id, key, version)

    # delete the folders, in reverse order
    folder_list.reverse()
    for o in folder_list:
        key = o.get("Key")
        version = o.get("VersionId")
        delete_object_version(s3_client, bucket_id, key, version)

def undo_actions(log, s3_client, bucket_id, dataset_id):
    log.info(f"undo_actions() bucket_id: {bucket_id} dataset_id: {dataset_id}")

    file_actions = load_file_actions(log, s3_client, bucket_id, dataset_id)

    if file_actions is None:
        log.info("undo_actions() file actions file not found ~> cannot undo actions")
        return

    for file_action in file_actions:
        # TODO: validate file_action (required: 4 fields)
        log.info(f"undo_actions() file_action: {file_action}")
        action = file_action.get(FileActionTag,FileActionUnknown)
        if action == FileActionCopy:
            undo_copy(log, s3_client, file_action)
        elif action == FileActionKeep:
            undo_keep(log, s3_client, file_action)
        elif action == FileActionDelete:
            undo_delete(log, s3_client, file_action)
        else:
            log.info(f"undo_actions() unsupported action: {action}")

def undo_copy(log, s3_client, file_action):
    log.info(f"undo_copy() file_action: {file_action}")
    s3_bucket = file_action.get(FileActionBucketTag)
    s3_key = file_action.get(FileActionPathTag)
    s3_version = file_action.get(FileActionVersionTag)

    # the s3_version on the FileAction represents the version we need to make the latest
    if s3_version is None or s3_version == "":
        # no S3 version on the FileAction indicates that this is the first time a file was to be
        # published to that path, so we need to remove any versions of the file
        delete_all_object_versions(log, s3_client, s3_bucket, s3_key)
    else:
        restore_version(log, s3_client, s3_bucket, s3_key, s3_version)

def undo_keep(log, s3_client, file_action):
    log.info(f"undo_keep() file_action: {file_action}")
    s3_bucket = file_action.get(FileActionBucketTag)
    s3_key = file_action.get(FileActionPathTag)
    s3_version = file_action.get(FileActionVersionTag)
    restore_version(log, s3_client, s3_bucket, s3_key, s3_version)

def undo_delete(log, s3_client, file_action):
    log.info(f"undo_delete() file_action: {file_action}")
    s3_bucket = file_action.get(FileActionBucketTag)
    s3_key = file_action.get(FileActionPathTag)
    s3_version = file_action.get(FileActionVersionTag)
    restore_version(log, s3_client, s3_bucket, s3_key, s3_version)

def restore_version(log, s3_client, s3_bucket, s3_key, s3_version):
    log.info(f"restore_version() bucket: {s3_bucket} key: {s3_key} version: {s3_version}")
    execute = True
    while execute:
        versions = get_object_versions(s3_client, s3_bucket, s3_key)
        latest = find_latest_version(versions)
        latest_version = latest.get(S3VersionIdTag, NoValue)
        if latest_version == s3_version:
            # the latest version is the desired version, so we are done
            log.info(f"restore_version() version {latest_version} is the latest")
            execute = False
        else:
            # the latest version is not the desired version, so remove it and check again
            log.info(f"restore_version() removing version: {latest_version}")
            delete_object_version(s3_client, s3_bucket, s3_key, latest_version)

def get_object_versions(s3_client, s3_bucket, s3_key):
    response = s3_client.list_object_versions(Bucket=s3_bucket, Prefix=s3_key)
    versions = extract_versions(response)
    return versions

def extract_versions(response):
    # extract Delete Markers and Versions from the response
    versions = (response.get(S3DeleteMarkersTag,[]) + response.get(S3VersionsTag,[]))
    # sort the Versions by timestamp (most recent to oldest)
    versions.sort(key = lambda x:x[S3LastModifiedTag])
    versions.reverse()
    return versions

def find_latest_version(versions):
    latest_list = []
    found = filter(is_latest, versions)
    for item in found:
        latest_list.append(item)
    if len(latest_list) == 1:
        return latest_list[0]
    else:
        return None

def is_latest(item):
    return item.get(S3IsLatestTag, False)

def load_file_actions(log, s3_client, bucket_id, dataset_id):
    s3_key = f"{dataset_id}/{FileActionKey}"
    try:
        s3_object = s3_client.get_object(Bucket=bucket_id, Key=s3_key)
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            log.info(f"load_file_actions() NoSuchKey - bucket: {bucket_id} key: {s3_key}")
            return None
        else:
            raise

    file_actions = json.loads(s3_object["Body"].read())
    return file_actions

def delete_all_object_versions(log, s3_client, s3_bucket, s3_key):
    log.info(f"delete_all_object_versions() bucket: {s3_bucket} key: {s3_key}")
    versions = get_object_versions(s3_client, s3_bucket, s3_key)
    for version in versions:
        s3_version = version.get(S3VersionIdTag)
        if s3_version is not None:
            delete_object_version(s3_client, s3_bucket, s3_key, s3_version)

def delete_object_version(s3_client, s3_bucket, s3_key, s3_version):
    s3_client.delete_object(Bucket=s3_bucket, Key=s3_key, VersionId=s3_version)
