import os
import json
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError

import structlog

@dataclass
class S3CleanConfig:
    """S3 Clean Invocation Config"""
    asset_bucket_id: str
    assets_prefix: str
    publish_bucket_id: str
    embargo_bucket_id: str
    s3_key_prefix: str
    cleanup_stage: str
    workflow_id: int
    dataset_id: str
    dataset_version: str
    tidy_enabled: bool

class WithLogging:
    def logger(class_name):
        log = structlog.get_logger()
        log.bind(**{'class': class_name})
        log.bind(pennsieve={'service_name': class_name})
        return log

class RequestPayer:
    def __init__(self, is_requestor_pays = True):
        self.is_requestor_pays = is_requestor_pays

    def __call__(self):
        return {'RequestPayer': 'requester'} if self.is_requestor_pays else {}

class S3Paginator:
    log = WithLogging.logger('S3Paginator')
    s3 = boto3.client("s3")
    paginator = None
    requestor_pays = None

    def init(paginator, is_requestor_pays = True):
        S3Paginator.log.info(f"init() is_requestor_pays: {is_requestor_pays}")
        S3Paginator.requestor_pays = RequestPayer(is_requestor_pays)
        S3Paginator.paginator = paginator

    def paginate(Bucket, Prefix, PaginationConfig={'PageSize': 1000}, **kwargs):
        S3Paginator.log.info(f"paginate() Bucket: {Bucket} Prefix: {Prefix} PaginationConfig: {PaginationConfig}")
        return S3Paginator.paginator.paginate(Bucket=Bucket,
                                              Prefix=Prefix,
                                              PaginationConfig=PaginationConfig,
                                              **S3Paginator.requestor_pays())

class S3Client:
    log = WithLogging.logger('S3Client')
    s3 = None
    is_requestor_pays = True
    requestor_pays = None

    def init(endpoint_url = None, is_requestor_pays = True):
        S3Client.log.info(f"init() is_requestor_pays: {is_requestor_pays}")
        S3Client.is_requestor_pays = is_requestor_pays
        S3Client.requestor_pays = RequestPayer(is_requestor_pays)
        S3Client.s3 = boto3.client("s3", endpoint_url=endpoint_url)

    def get_paginator(operation_name):
        S3Client.log.info(f"get_paginator() operation_name: {operation_name}")
        S3Paginator.init(S3Client.s3.get_paginator(operation_name),
                         S3Client.is_requestor_pays)
        return S3Paginator

    def list_object_versions(Bucket, Prefix):
        S3Client.log.info(f"list_object_versions() Bucket: {Bucket} Prefix: {Prefix}")
        return S3Client.s3.list_object_versions(Bucket=Bucket,
                                                Prefix=Prefix,
                                                **S3Client.requestor_pays())

    def put_object(Body, Bucket, Key):
        S3Client.log.info(f"put_object() Bucket: {Bucket} Key: {Key}")
        return S3Client.s3.put_object(Body=Body,
                                      Bucket=Bucket,
                                      Key=Key,
                                      **S3Client.requestor_pays())

    def get_object(Bucket, Key):
        S3Client.log.info(f"get_object() Bucket: {Bucket} Key: {Key}")
        return S3Client.s3.get_object(Bucket=Bucket,
                                      Key=Key,
                                      **S3Client.requestor_pays())

    def delete_object(Bucket, Key, VersionId=None):
        S3Client.log.info(f"delete_object() Bucket: {Bucket} Key: {Key} VersionId: {VersionId}")
        if VersionId is not None:
            return S3Client.s3.delete_object(Bucket=Bucket,
                                             Key=Key,
                                             VersionId=VersionId,
                                             **S3Client.requestor_pays())
        else:
            return S3Client.s3.delete_object(Bucket=Bucket,
                                             Key=Key,
                                             **S3Client.requestor_pays())

    def delete_objects(Bucket, Delete, **kwargs):
        S3Client.log.info(f"delete_objects() Bucket: {Bucket} number-of-items: {len(Delete)}")
        return S3Client.s3.delete_objects(Bucket=Bucket, Delete=Delete, **S3Client.requestor_pays())

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

S3DeleteMarkersTag = "DeleteMarkers"
S3VersionsTag = "Versions"
S3LastModifiedTag = "LastModified"
S3IsLatestTag = "IsLatest"
S3VersionIdTag = "VersionId"

CleanupStageInitial = "INITIAL"
CleanupStageFailure = "FAILURE"
CleanupStageUnpublish = "UNPUBLISH"
CleanupStageTidy = "TIDY"

FileActionKey = "file-actions.json"
DatasetAssetsKey = "publish.json"
GraphAssetsKey = "graph.json"
OutputAssetsKey = "outputs.json"
RevisionsCleanupKey = "cleanup-revisions.json"
MetadataCleanupKey = "cleanup-metadata.json"

RevisionsPrefix = "revisions"
MetadataPrefix = "metadata"

FileActionTag = "action"
FileActionBucketTag = "bucket"
FileActionPathTag = "path"
FileActionVersionTag = "versionId"
FileActionRequiredFields = [FileActionTag, FileActionBucketTag, FileActionPathTag]

FileActionListTag = "fileActionList"

FileActionCopy = "CopyFile"
FileActionKeep = "KeepFile"
FileActionDelete = "DeleteFile"
FileActionUnknown = "Unknown"

Default_TidyEnabled = True

NoValue = "(none)"

PublishingIntermediateFiles = [FileActionKey,
                               GraphAssetsKey,
                               OutputAssetsKey,
                               DatasetAssetsKey,
                               RevisionsCleanupKey,
                               MetadataCleanupKey]

def str_to_bool(s):
    if s is not None:
        return s.upper() == "TRUE"
    else:
        return False

def is_tidy_enabled(tidy_enabled_evt, tidy_enabled_env):
    if tidy_enabled_evt is not None:
        return str_to_bool(tidy_enabled_evt)
    elif tidy_enabled_env is not None:
        return str_to_bool(tidy_enabled_env)
    else:
        return Default_TidyEnabled

def lambda_handler(event, context, s3_client=S3_CLIENT, s3_paginator=PAGINATOR):
    # Create basic Pennsieve log context
    log = structlog.get_logger()
    log = log.bind(**{'class': f'{lambda_handler.__module__}.{lambda_handler.__name__}'})
    log = log.bind(pennsieve={'service_name': FULL_SERVICE_NAME})

    try:
        log.info('Reading environment')
        asset_bucket_id = os.environ['ASSET_BUCKET']
        assets_prefix = os.environ['DATASET_ASSETS_KEY_PREFIX']
        tidy_enabled_env = os.environ.get("TIDY_ENABLED","TRUE")

        log.info('Parsing event')

        publish_bucket_id = event['publish_bucket']
        embargo_bucket_id = event['embargo_bucket']
        s3_key_prefix = event['s3_key_prefix']
        cleanup_stage = event.get("cleanup_stage", CleanupStageInitial)
        workflow_id = int(event.get("workflow_id", "4"))
        dataset_id = event.get("published_dataset_id","-1")
        dataset_version = event.get("published_dataset_version", "-1")
        tidy_enabled_evt = event.get("tidy_enabled")

        tidy_enabled = is_tidy_enabled(tidy_enabled_evt, tidy_enabled_env)

        s3_clean_config = S3CleanConfig(asset_bucket_id, assets_prefix, publish_bucket_id, embargo_bucket_id, s3_key_prefix, cleanup_stage, workflow_id, dataset_id, dataset_version, tidy_enabled)

        S3Client.init(endpoint_url=S3_URL, is_requestor_pays=True)
        S3Paginator = S3Client.get_paginator('list_objects_v2')

        if workflow_id == 5:
            purge_v5(log, S3Client, S3Paginator, s3_clean_config)
        else:
            if cleanup_stage == CleanupStageTidy:
                tidy_v4(log, tidy_enabled, S3Client, publish_bucket_id, embargo_bucket_id, s3_key_prefix)
            else:
                purge_v4(log, asset_bucket_id, assets_prefix, publish_bucket_id, embargo_bucket_id, s3_key_prefix, S3Client, S3Paginator)

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

def tidy_v4(log, tidy_enabled, s3_client, publish_bucket_id, embargo_bucket_id, s3_key_prefix):
    log.info(f"tidy_v4() tidy_enabled: {tidy_enabled} publish_bucket_id: {publish_bucket_id} embargo_bucket_id: {embargo_bucket_id} s3_key_prefix: {s3_key_prefix}")
    if tidy_enabled:
        log.info(f"tidy_v4() removing intermediate publishing files")
        for bucket_id in [publish_bucket_id, embargo_bucket_id]:
            tidy_publication_directory(log, s3_client, bucket_id, s3_key_prefix)
    else:
        log.info(f"tidy_v4() requested but disabled")

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

def purge_v5(log, s3_client, s3_paginator, s3_clean_config):
    log.info(f"purge_v5() {s3_clean_config.cleanup_stage} config: {s3_clean_config}")

    if s3_clean_config.cleanup_stage == CleanupStageInitial:
        purge_v5_initial(log, s3_client, s3_clean_config)

    if s3_clean_config.cleanup_stage == CleanupStageTidy:
        purge_v5_tidy(log, s3_client, s3_clean_config)

    if s3_clean_config.cleanup_stage == CleanupStageUnpublish:
        purge_v5_unpublish(log, s3_client, s3_paginator, s3_clean_config)

    if s3_clean_config.cleanup_stage == CleanupStageFailure:
        purge_v5_failure(log, s3_client, s3_paginator, s3_clean_config)

def purge_v5_initial(log, s3_client, s3_clean_config):
    log.info(f"purge_v5_initial() preparing space for publication")
    cleanup_dataset_revisions(log, s3_client, s3_clean_config)
    cleanup_dataset_metadata(log, s3_client, s3_clean_config)

def purge_v5_tidy(log, s3_client, s3_clean_config):
    if s3_clean_config.tidy_enabled:
        log.info(f"purge_v5_tidy() removing intermediate publishing files")
        for bucket_id in [s3_clean_config.publish_bucket_id, s3_clean_config.embargo_bucket_id]:
            tidy_publication_directory(log, s3_client, bucket_id, s3_clean_config.s3_key_prefix)
    else:
        log.info(f"purge_v5_tidy() requested but disabled")

def purge_v5_unpublish(log, s3_client, s3_paginator, s3_clean_config):
    log.info(f"purge_v5_unpublish() will remove all versions and all files")

    for bucket_id in [s3_clean_config.publish_bucket_id, s3_clean_config.embargo_bucket_id]:
        delete_all_versions(log, s3_client, bucket_id, s3_clean_config.dataset_id)

    # Delete all files in the Public Assets Bucket
    cleanup_public_assets_bucket(log,
                                 s3_client,
                                 s3_paginator,
                                 s3_clean_config.asset_bucket_id,
                                 s3_clean_config.assets_prefix,
                                 s3_clean_config.dataset_id,
                                 None)

def purge_v5_failure(log, s3_client, s3_paginator, s3_clean_config):
    log.info(f"purge_v5_failure() undo publishing actions and clean public assets bucket")

    for bucket_id in [s3_clean_config.publish_bucket_id, s3_clean_config.embargo_bucket_id]:
        log.info(f"purge_v5_failure() undo publishing in bucket_id: {bucket_id}")
        delete_dataset_assets(log, s3_client, bucket_id, s3_clean_config.dataset_id)
        delete_graph_assets(log, s3_client, bucket_id, s3_clean_config.dataset_id)
        undo_actions(log, s3_client, bucket_id, s3_clean_config.dataset_id)
        if s3_clean_config.tidy_enabled:
            tidy_publication_directory(log, s3_client, bucket_id, s3_clean_config.s3_key_prefix)

    # Clean up the Public Assets Bucket
    cleanup_public_assets_bucket(log,
                                 s3_client,
                                 s3_paginator,
                                 s3_clean_config.asset_bucket_id,
                                 s3_clean_config.assets_prefix,
                                 s3_clean_config.dataset_id,
                                 s3_clean_config.dataset_version)

def cleanup_dataset_revisions(log, s3_client, s3_clean_config):
    log.info(f"cleanup_dataset_revisions() {s3_clean_config.dataset_id}")
    cleanup_dataset_folders(log,
                            s3_client,
                            [s3_clean_config.publish_bucket_id, s3_clean_config.embargo_bucket_id],
                            s3_clean_config.dataset_id,
                            RevisionsPrefix,
                            RevisionsCleanupKey)

def cleanup_dataset_metadata(log, s3_client, s3_clean_config):
    log.info(f"cleanup_dataset_metadata() {s3_clean_config.dataset_id}")
    cleanup_dataset_folders(log,
                            s3_client,
                            [s3_clean_config.publish_bucket_id, s3_clean_config.embargo_bucket_id],
                            s3_clean_config.dataset_id,
                            MetadataPrefix,
                            MetadataCleanupKey)

def cleanup_dataset_folders(log, s3_client, bucket_list, dataset_id, folder_prefix, folder_cleanup_key):
    log.info(f"cleanup_dataset_folders() dataset_id: {dataset_id} folder_prefix: {folder_prefix} folder_cleanup_key: {folder_cleanup_key} bucket_list: {bucket_list}")
    key_prefix = f"{dataset_id}/{folder_prefix}"
    cleanup_file = f"{dataset_id}/{folder_cleanup_key}" if folder_cleanup_key is not None else None
    cleanup_buckets(log,
                    s3_client,
                    bucket_list,
                    key_prefix,
                    cleanup_file)

def cleanup_buckets(log, s3_client, bucket_list, key_prefix, cleanup_file):
    log.info(f"cleanup_buckets() key_prefix: {key_prefix} cleanup_file: {cleanup_file} bucket_list: {bucket_list}")

    for bucket_id in bucket_list:
        log.info(f"cleanup_buckets() processing bucket_id: {bucket_id}")
        file_actions = remove_files_from_bucket(log, s3_client, bucket_id, key_prefix)
        if file_actions is not None and len(file_actions.get(FileActionListTag, [])) > 0:
            log.info(f"cleanup_buckets() bucket_id: {bucket_id} cleaned up {len(file_actions.get(FileActionListTag))} files")
            write_json_file_to_s3(log, s3_client, bucket_id, cleanup_file, json.dumps(file_actions))

def remove_files_from_bucket(log, s3_client, bucket_id, key_prefix):
    log.info(f"remove_files_from_bucket() bucket_id: {bucket_id} key_prefix: {key_prefix}")

    file_list = get_list_of_files(log, s3_client, bucket_id, key_prefix)
    log.info(f"remove_files_from_bucket() will delete {len(file_list)} files")
    file_action_list = [remove_file(log, s3_client, bucket_id, file) for file in file_list]
    return {FileActionListTag: file_action_list}

def cleanup_public_assets_bucket(log, s3_client, s3_paginator, bucket_id, prefix, dataset_id, version_id = None):
    log.info(f"cleanup_public_assets_bucket() bucket_id: {bucket_id} prefix: {prefix} dataset_id: {dataset_id} version_id: {version_id}")
    dataset_assets_prefix = public_assets_prefix(prefix, dataset_id, version_id)
    delete(s3_client, s3_paginator, bucket_id, dataset_assets_prefix)

def get_list_of_files(log, s3_client, bucket_id, prefix):
    '''
    Gets a list of current files in the S3 Bucket with the specified prefix. Uses a Paginator.
    :param log: a logger
    :param s3_client: an S3 Client
    :param bucket_id: the name of the S3 Bucket
    :param prefix: the S3 object prefix
    :return: a list of current files (IsLatest == true), in AWS Response format (ETag, Key, Size, VersionId, IsLatest, etc.)
    '''
    log.info(f"get_list_of_files() bucket_id: {bucket_id} prefix: {prefix}")
    paginator = s3_client.get_paginator('list_object_versions')
    bucket_listing = [file
                      for page in paginator.paginate(Bucket=bucket_id, Prefix=prefix, PaginationConfig={'PageSize': 1000})
                      for file in page.get("Versions", []) if file.get("IsLatest")]
    return bucket_listing

def remove_file(log, s3_client, bucket_id, file):
    '''
    Deletes a current file from S3. Specifically used for cleaning up folders, a recovery action is returned. This is not a general-purpose delete function.
    :param log: a logger
    :param s3_client: an S3 Client
    :param bucket_id: the name of the S3 Bucket
    :param file: the S3 object to be deleted (must have a Key and VersionId)
    :return: a FileActionItem (action, bucket, path, versionId)
    '''
    key = file.get("Key")
    version = file.get("VersionId")
    log.info(f"remove_file() bucket_id: {bucket_id} key: {key} version: {version}")
    delete_object(log, s3_client, bucket_id, key)
    return {
        "action": FileActionDelete,
        "bucket": bucket_id,
        "path": key,
        "versionId": version
    }

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

def delete_dataset_assets(log, s3_client, s3_bucket, dataset_id):
    '''
    This function will remove versions of the dataset assets (banner, readme, manifest.json) that were copied to S3 as part of the publishing process.
    :param log: logger
    :param s3_client: an S3 client
    :param s3_bucket: the name of the S3 Bucket
    :param dataset_id: the published dataset id
    :return: (none)
    '''
    log.info(f"delete_dataset_assets() s3_bucket: {s3_bucket} dataset_id: {dataset_id}")
    s3_asset_key = s3_key_path(dataset_id, DatasetAssetsKey)
    dataset_assets = load_json_file_from_s3(log, s3_client, s3_bucket, s3_asset_key)
    if dataset_assets is not None:
        for tag in ["bannerManifest", "readmeManifest", "changelogManifest"]:
            log.info(f"delete_dataset_assets() looking for tag: {tag}")
            manifest = dataset_assets.get(tag)
            if manifest is not None:
                log.info(f"delete_dataset_assets() found manifest: {manifest}")
                s3_path = manifest.get("path")
                s3_key = s3_key_path(dataset_id, s3_path)
                s3_version = manifest.get("s3VersionId")
                delete_object_version(s3_client, s3_bucket, s3_key, s3_version)

def delete_graph_assets(log, s3_client, s3_bucket, dataset_id):
    '''
    This will delete versions of the graph assets (schemas, models, records) that were copied to the S3 bucket.
    :param log: logger
    :param s3_client: an S3 client
    :param s3_bucket: the name of the S3 bucket
    :param dataset_id: the published dataset id
    :return: (none)
    '''
    log.info(f"delete_graph_assets() s3_bucket: {s3_bucket} dataset_id: {dataset_id}")
    s3_asset_key = s3_key_path(dataset_id, GraphAssetsKey)
    graph_assets = load_json_file_from_s3(log, s3_client, s3_bucket, s3_asset_key)
    if graph_assets is not None:
        manifests = graph_assets.get("manifests")
        if manifests is not None:
            for manifest in manifests:
                log.info(f"delete_graph_assets() manifest: {manifest}")
                s3_path = manifest.get("path")
                s3_key = s3_key_path(dataset_id, s3_path)
                s3_version = manifest.get("s3VersionId")
                delete_object_version(s3_client, s3_bucket, s3_key, s3_version)

def undo_actions(log, s3_client, bucket_id, dataset_id):
    '''
    This will undo the actions performed during the dataset publishing process. It will remove new files copied, and restore files that were deleted or replaced.
    :param log: logger
    :param s3_client: an S3 client
    :param bucket_id: the name of the publishing S3 bucket
    :param dataset_id: the published dataset id
    :return: (none)
    '''
    log.info(f"undo_actions() bucket_id: {bucket_id} dataset_id: {dataset_id}")

    file_actions = load_dataset_file_actions(log, s3_client, bucket_id, dataset_id)
    log.info(f"undo_actions() there are {len(file_actions)} file actions to undo")

    for file_action in file_actions:
        if valid_file_action(file_action):
            log.info(f"undo_actions() process file_action: {file_action}")
            action = file_action.get(FileActionTag, FileActionUnknown)
            if action == FileActionCopy:
                undo_copy(log, s3_client, file_action)
            elif action == FileActionKeep:
                undo_keep(log, s3_client, file_action)
            elif action == FileActionDelete:
                undo_delete(log, s3_client, file_action)
            else:
                log.info(f"undo_actions() unsupported action: {action}")
        else:
            log.info(f"undo_actions() invalid file_action: {file_action}")

def valid_file_action(file_action):
    return all([field in file_action for field in FileActionRequiredFields])

def tidy_publication_directory(log, s3_client, s3_bucket_id, s3_key_prefix):
    log.info(f"tidy_publication_directory() s3_bucket_id: {s3_bucket_id} s3_key_prefix: {s3_key_prefix}")
    for file_name in PublishingIntermediateFiles:
        s3_key = s3_key_path(s3_key_prefix, file_name)
        delete_all_object_versions(log, s3_client, s3_bucket_id, s3_key)

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
    if s3_version is not None:
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
    else:
        log.info(f"restore_version() cannot restore without a valid object version (bucket: {s3_bucket} key: {s3_key} version: {s3_version})")

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

def write_json_file_to_s3(log, s3_client, bucket, key, json_data):
    log.info(f"write_json_file_to_s3() bucket: {bucket} key: {bucket}")
    response = s3_client.put_object(
        Body=json_data,
        Bucket=bucket,
        Key=key
    )
    # TODO: check response for success/failure

def load_json_file_from_s3(log, s3_client, s3_bucket, s3_key):
    '''
    General purpose function to read a JSON file from S3.
    :param log: logger
    :param s3_client: an S3 client
    :param s3_bucket: the name of the S3 bucket
    :param s3_key: S3 Key of the file
    :return: JSON in dict() format
    '''
    log.info(f"load_json_file_from_s3() s3_bucket: {s3_bucket} s3_key: {s3_key}")
    try:
        s3_object = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            log.info(f"load_json_file_from_s3() NoSuchKey - bucket: {s3_bucket} key: {s3_key}")
            return None
        else:
            raise

    json_file = json.loads(s3_object["Body"].read())
    return json_file

def load_dataset_file_actions(log, s3_client, bucket_id, dataset_id):
    '''
    Loads files from the publishing S3 bucket that contain FileActionItem (copy, keep, delete), from publishing and revision cleanup.
    :param log: logger
    :param s3_client: an S3 client
    :param bucket_id: the name of the S3 bucket
    :param dataset_id: the published dataset id
    :return: combined List of File Actions
    '''
    return load_file_actions(log, s3_client, bucket_id, dataset_id, FileActionKey) + \
           load_file_actions(log, s3_client, bucket_id, dataset_id, RevisionsCleanupKey) + \
           load_file_actions(log, s3_client, bucket_id, dataset_id, MetadataCleanupKey)

def load_file_actions(log, s3_client, bucket_id, dataset_id, file_action_key):
    '''
    Loads a File Actions file from S3. The file contains a list File Actions serialized to JSON.
    :param log: logger
    :param s3_client: and S3 client
    :param bucket_id: the name of the S3 bucket
    :param dataset_id: the published dataset id
    :param file_action_key: the S3 Key of the file to be loaded
    :return: List of File Actions
    '''
    s3_key = f"{dataset_id}/{file_action_key}"
    log.info(f"load_file_actions() bucket_id: {bucket_id} dataset_id: {dataset_id} s3_key: {s3_key}")
    json_data = load_json_file_from_s3(log, s3_client, bucket_id, s3_key)
    if json_data is not None:
        return json_data.get(FileActionListTag, [])
    else:
        log.info(f"load_file_actions() NotFound bucket_id: {bucket_id} dataset_id: {dataset_id} s3_key: {s3_key}")
        return []

def delete_all_object_versions(log, s3_client, s3_bucket, s3_key):
    log.info(f"delete_all_object_versions() bucket: {s3_bucket} key: {s3_key}")
    versions = get_object_versions(s3_client, s3_bucket, s3_key)
    for version in versions:
        s3_version = version.get(S3VersionIdTag)
        if s3_version is not None:
            if s3_version == "null":
                delete_object(log, s3_client, s3_bucket, s3_key)
            else:
                delete_object_version(s3_client, s3_bucket, s3_key, s3_version)

def delete_object(log, s3_client, s3_bucket, s3_key):
    log.info(f"delete_object() s3_bucket: {s3_bucket} s3_key: {s3_key}")
    s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)

def delete_object_version(s3_client, s3_bucket, s3_key, s3_version):
    s3_client.delete_object(Bucket=s3_bucket, Key=s3_key, VersionId=s3_version)

def public_assets_prefix(prefix, dataset_id, version_id):
    if version_id is None:
        return f"{prefix}/{dataset_id}"
    else:
        return f"{prefix}/{dataset_id}/{version_id}"

def s3_key_path(prefix, suffix):
    separator = "" if prefix.endswith("/") else "/"
    return f"{prefix}{separator}{suffix}"
