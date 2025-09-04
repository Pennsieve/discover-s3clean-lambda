import io
import json
from ssl import create_default_context

import boto3
import os
import pytest
import time

from botocore.retryhandler import create_exponential_delay_function

# When running 'make test', these env vars are set by Docker in the docker-compose file.
# This block is here for the case where a dev would like to run the tests directly instead
# of in Docker. This needs to be before the first import from main, since main will fail to
# load if they are not set.
#
# To run the tests in your IDE, first start localstack however you like, making sure to expose port 4566 and passing
# it the env var SERVICES=s3. Then you can run pytest -s test.py or just run tests individually in your IDE.
if 'ENVIRONMENT' not in os.environ:
    os.environ['ENVIRONMENT'] = 'local'
    os.environ['SERVICE_NAME'] = 'discover'
    os.environ['TIER'] = 's3clean'

from main import lambda_handler, S3_URL, CleanupStageInitial, RevisionsPrefix, RevisionsCleanupKey, MetadataPrefix, \
    MetadataCleanupKey, CleanupStageTidy, PublishingIntermediateFiles, CleanupStageUnpublish, \
    CleanupStageFailure, DatasetAssetsKey, GraphAssetsKey, FileActionKey, FileActionListTag, FileActionTag, \
    FileActionBucketTag, FileActionPathTag, FileActionVersionTag, FileActionCopy

PUBLISH_BUCKET = 'test-discover-publish'
EMBARGO_BUCKET = 'test-discover-embargo'
ASSET_BUCKET = 'test-discover-assets'
DATASET_ASSETS_KEY_PREFIX = 'dataset-assets'

# This key corresponds to assets belonging to a dataset
# that is either being unpublished or was not published successfully
S3_PREFIX_TO_DELETE = '11'

# This key corresponds to assets belonging to a dataset version
# that should remain untouched by this lambda function
S3_PREFIX_TO_KEEP = '111'

# This is a dummy file
FILENAME = 'test.txt'

s3_resource = boto3.resource('s3', endpoint_url=S3_URL)


@pytest.fixture(scope='module')
def setup():
    os.environ.update({
        'ASSET_BUCKET': ASSET_BUCKET,
        'DATASET_ASSETS_KEY_PREFIX': DATASET_ASSETS_KEY_PREFIX,
    })

    time.sleep(5)  # let localstack spin up


@pytest.fixture(scope='function')
def publish_bucket(setup):
    return setup_bucket(PUBLISH_BUCKET, is_versioned=True)


@pytest.fixture(scope='function')
def embargo_bucket(setup):
    return setup_bucket(EMBARGO_BUCKET, is_versioned=False)


@pytest.fixture(scope='function')
def asset_bucket(setup):
    return setup_bucket(ASSET_BUCKET, is_versioned=False)


def test_empty_dataset(publish_bucket, embargo_bucket, asset_bucket):
    s3_key_to_keep = '{}/{}'.format(S3_PREFIX_TO_KEEP, FILENAME)
    asset_key_to_keep = '{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, s3_key_to_keep)

    publish_bucket.upload_file(Filename=FILENAME, Key=s3_key_to_keep)
    embargo_bucket.upload_file(Filename=FILENAME, Key=s3_key_to_keep)
    asset_bucket.upload_file(Filename=FILENAME, Key=asset_key_to_keep)

    assert s3_keys(publish_bucket) == {s3_key_to_keep}
    assert s3_keys(embargo_bucket) == {s3_key_to_keep}
    assert s3_keys(asset_bucket) == {asset_key_to_keep}

    # RUN LAMBDA
    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE,
        'publish_bucket': PUBLISH_BUCKET,
        'embargo_bucket': EMBARGO_BUCKET
    }, {})

    # VERIFY RESULTS
    assert s3_keys(publish_bucket) == {s3_key_to_keep}
    assert s3_keys(embargo_bucket) == {s3_key_to_keep}
    assert s3_keys(asset_bucket) == {asset_key_to_keep}


def test_large_dataset_for_publish_bucket(publish_bucket, embargo_bucket, asset_bucket):
    s3_keys_to_delete = create_keys(S3_PREFIX_TO_DELETE, FILENAME)
    s3_key_to_keep = '{}/{}'.format(S3_PREFIX_TO_KEEP, FILENAME)
    asset_key_to_delete = '{}/{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, S3_PREFIX_TO_DELETE, FILENAME)
    asset_key_to_keep = '{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, s3_key_to_keep)

    for key in s3_keys_to_delete:
        publish_bucket.upload_file(Filename=FILENAME, Key=key)
        embargo_bucket.upload_file(Filename=FILENAME, Key=key)
    asset_bucket.upload_file(Filename=FILENAME, Key=asset_key_to_delete)

    publish_bucket.upload_file(Filename=FILENAME, Key=s3_key_to_keep)
    embargo_bucket.upload_file(Filename=FILENAME, Key=s3_key_to_keep)
    asset_bucket.upload_file(Filename=FILENAME, Key=asset_key_to_keep)

    expected_keys = s3_keys_to_delete
    expected_keys.append(s3_key_to_keep)
    assert s3_keys(publish_bucket) == set(expected_keys)
    assert s3_keys(embargo_bucket) == set(expected_keys)
    assert s3_keys(asset_bucket) == {asset_key_to_delete, asset_key_to_keep}

    # RUN LAMBDA
    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE,
        'publish_bucket': PUBLISH_BUCKET,
        'embargo_bucket': EMBARGO_BUCKET
    }, {})

    # VERIFY RESULTS
    assert s3_keys(publish_bucket) == {s3_key_to_keep}
    assert s3_keys(embargo_bucket) == {s3_key_to_keep}
    assert s3_keys(asset_bucket) == {asset_key_to_keep}


def test_handle_input_with_trailing_slash(publish_bucket, embargo_bucket, asset_bucket):
    s3_key_to_delete = '{}/{}'.format(S3_PREFIX_TO_DELETE, FILENAME)
    s3_key_to_keep = '{}/{}'.format(S3_PREFIX_TO_KEEP, FILENAME)
    asset_key_to_delete = '{}/{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, S3_PREFIX_TO_DELETE, FILENAME)
    asset_key_to_keep = '{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, s3_key_to_keep)

    publish_bucket.upload_file(Filename=FILENAME, Key=s3_key_to_delete)
    embargo_bucket.upload_file(Filename=FILENAME, Key=s3_key_to_delete)
    asset_bucket.upload_file(Filename=FILENAME, Key=asset_key_to_delete)

    publish_bucket.upload_file(Filename=FILENAME, Key=s3_key_to_keep)
    embargo_bucket.upload_file(Filename=FILENAME, Key=s3_key_to_keep)
    asset_bucket.upload_file(Filename=FILENAME, Key=asset_key_to_keep)

    expected_keys = {s3_key_to_delete, s3_key_to_keep}
    assert s3_keys(publish_bucket) == expected_keys
    assert s3_keys(embargo_bucket) == expected_keys
    assert s3_keys(asset_bucket) == {asset_key_to_delete, asset_key_to_keep}

    # RUN LAMBDA
    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE + "/",
        'publish_bucket': PUBLISH_BUCKET,
        'embargo_bucket': EMBARGO_BUCKET,
    }, {})

    # VERIFY RESULTS
    assert s3_keys(publish_bucket) == {s3_key_to_keep}
    assert s3_keys(embargo_bucket) == {s3_key_to_keep}
    assert s3_keys(asset_bucket) == {asset_key_to_keep}


def test_include_requestor_pays():
    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE,
        'publish_bucket': PUBLISH_BUCKET,
        'embargo_bucket': EMBARGO_BUCKET,
    }, {}, s3_client=MockClient(), s3_paginator=MockPaginator())


def test_cleanup_state_initial(publish_bucket, embargo_bucket):
    revision_key_to_delete = '{}/{}/{}'.format(S3_PREFIX_TO_DELETE, RevisionsPrefix, FILENAME)
    other_dataset_revision_key_to_keep = '{}/{}/{}'.format(S3_PREFIX_TO_KEEP, RevisionsPrefix, FILENAME)

    metadata_key_to_delete = '{}/{}/{}'.format(S3_PREFIX_TO_DELETE, MetadataPrefix, FILENAME)
    other_dataset_metadata_key_to_keep = '{}/{}/{}'.format(S3_PREFIX_TO_KEEP, MetadataPrefix, FILENAME)

    # files outside of revisions and metadata should be untouched in the initial cleanup stage
    file_key_to_keep = '{}/{}'.format(S3_PREFIX_TO_DELETE, FILENAME)

    publish_bucket.upload_file(Filename=FILENAME, Key=file_key_to_keep)
    embargo_bucket.upload_file(Filename=FILENAME, Key=file_key_to_keep)

    publish_bucket.upload_file(Filename=FILENAME, Key=revision_key_to_delete)
    publish_bucket.upload_file(Filename=FILENAME, Key=other_dataset_revision_key_to_keep)
    publish_bucket.upload_file(Filename=FILENAME, Key=metadata_key_to_delete)
    publish_bucket.upload_file(Filename=FILENAME, Key=other_dataset_metadata_key_to_keep)

    embargo_bucket.upload_file(Filename=FILENAME, Key=revision_key_to_delete)
    embargo_bucket.upload_file(Filename=FILENAME, Key=other_dataset_revision_key_to_keep)
    embargo_bucket.upload_file(Filename=FILENAME, Key=metadata_key_to_delete)
    embargo_bucket.upload_file(Filename=FILENAME, Key=other_dataset_metadata_key_to_keep)

    pre_clean_expected_keys = {
        revision_key_to_delete,
        other_dataset_revision_key_to_keep,
        metadata_key_to_delete,
        other_dataset_metadata_key_to_keep,
        file_key_to_keep
    }
    assert s3_keys(publish_bucket) == pre_clean_expected_keys
    assert s3_keys(embargo_bucket) == pre_clean_expected_keys

    # RUN LAMBDA
    lambda_handler({
        'published_dataset_id': S3_PREFIX_TO_DELETE,
        'publish_bucket': PUBLISH_BUCKET,
        'embargo_bucket': EMBARGO_BUCKET,
        'workflow_id': '5',
        'cleanup_stage': CleanupStageInitial
    }, {})

    post_clean_expected_keys = {
        other_dataset_revision_key_to_keep,
        other_dataset_metadata_key_to_keep,
        file_key_to_keep,
        '{}/{}'.format(S3_PREFIX_TO_DELETE, RevisionsCleanupKey),
        '{}/{}'.format(S3_PREFIX_TO_DELETE, MetadataCleanupKey),
    }

    assert s3_keys(publish_bucket) == post_clean_expected_keys
    assert s3_keys(embargo_bucket) == post_clean_expected_keys


def test_cleanup_state_tidy(publish_bucket, embargo_bucket):
    intermediate_keys_to_delete = ['{}/{}'.format(S3_PREFIX_TO_DELETE, x) for x in PublishingIntermediateFiles]
    intermediate_keys_to_keep = ['{}/{}'.format(S3_PREFIX_TO_KEEP, x) for x in PublishingIntermediateFiles]

    # a key in the dataset being cleaned that tidy should ignore
    untouched_key = '{}/{}'.format(S3_PREFIX_TO_DELETE, FILENAME)

    for key in intermediate_keys_to_keep:
        publish_bucket.upload_file(Filename=FILENAME, Key=key)
        embargo_bucket.upload_file(Filename=FILENAME, Key=key)
    for key in intermediate_keys_to_delete:
        publish_bucket.upload_file(Filename=FILENAME, Key=key)
        embargo_bucket.upload_file(Filename=FILENAME, Key=key)

    publish_bucket.upload_file(FILENAME, untouched_key)
    embargo_bucket.upload_file(FILENAME, untouched_key)

    pre_clean_expected_keys = set(intermediate_keys_to_keep + intermediate_keys_to_delete + [untouched_key])
    assert s3_keys(publish_bucket) == pre_clean_expected_keys
    assert s3_keys(embargo_bucket) == pre_clean_expected_keys

    # RUN LAMBDA
    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE,
        'publish_bucket': PUBLISH_BUCKET,
        'embargo_bucket': EMBARGO_BUCKET,
        'workflow_id': '5',
        'cleanup_stage': CleanupStageTidy
    }, {})

    post_clean_expected_keys = set(intermediate_keys_to_keep + [untouched_key])

    assert s3_keys(publish_bucket) == post_clean_expected_keys
    assert s3_keys(embargo_bucket) == post_clean_expected_keys


def test_cleanup_state_unpublish(publish_bucket, embargo_bucket, asset_bucket):
    keys_to_delete = create_keys(S3_PREFIX_TO_DELETE, FILENAME, count=5) + create_keys(
        '{}/{}'.format(S3_PREFIX_TO_DELETE, 'files'), FILENAME, count=7)
    keys_to_keep = create_keys(S3_PREFIX_TO_KEEP, FILENAME, count=3) + create_keys(
        '{}/{}'.format(S3_PREFIX_TO_KEEP, 'files'), FILENAME, count=2)

    # Version 1
    for key in keys_to_keep:
        publish_bucket.upload_file(Filename=FILENAME, Key=key)
        embargo_bucket.upload_file(Filename=FILENAME, Key=key)
    for key in keys_to_delete:
        publish_bucket.upload_file(Filename=FILENAME, Key=key)
        embargo_bucket.upload_file(Filename=FILENAME, Key=key)

    # Version 2
    for key in keys_to_keep:
        publish_bucket.upload_file(Filename=FILENAME, Key=key)
    for key in keys_to_delete:
        publish_bucket.upload_file(Filename=FILENAME, Key=key)

    pre_clean_expected_keys = set(keys_to_keep + keys_to_delete)
    assert s3_keys(publish_bucket) == pre_clean_expected_keys
    assert s3_keys(embargo_bucket) == pre_clean_expected_keys

    # Assets too
    assets_to_delete = create_keys('{}/{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, S3_PREFIX_TO_DELETE, 1), FILENAME,
                                   count=4) + create_keys(
        '{}/{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, S3_PREFIX_TO_DELETE, 2), FILENAME,
        count=4)

    assets_to_keep = create_keys('{}/{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, S3_PREFIX_TO_KEEP, 1), FILENAME,
                                 count=4) + create_keys(
        '{}/{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, S3_PREFIX_TO_KEEP, 2), FILENAME,
        count=4)
    for key in assets_to_delete + assets_to_keep:
        asset_bucket.upload_file(Filename=FILENAME, Key=key)

    assert s3_keys(asset_bucket) == set(assets_to_keep + assets_to_delete)

    # RUN LAMBDA
    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE,
        'published_dataset_id': S3_PREFIX_TO_DELETE,
        'publish_bucket': PUBLISH_BUCKET,
        'embargo_bucket': EMBARGO_BUCKET,
        'workflow_id': '5',
        'cleanup_stage': CleanupStageUnpublish
    }, {})

    post_clean_expected_keys = set(keys_to_keep)
    assert s3_keys(publish_bucket) == post_clean_expected_keys
    assert s3_keys(embargo_bucket) == post_clean_expected_keys

    # verify there are no versions hidden under delete markers
    for key in keys_to_delete:
        assert len(list(publish_bucket.object_versions.filter(Prefix=key))) == 0

    assert s3_keys(asset_bucket) == set(assets_to_keep)


# this does not test the handling of file actions
def test_cleanup_state_failure(publish_bucket, embargo_bucket, asset_bucket):
    dataset_version = 1

    publish_keys, asset_keys = create_publish_files(publish_bucket,
                                                    embargo_bucket,
                                                    asset_bucket,
                                                    S3_PREFIX_TO_DELETE,
                                                    dataset_version)

    publish_keys_to_keep, asset_keys_to_keep = create_publish_files(publish_bucket,
                                                                    embargo_bucket,
                                                                    asset_bucket,
                                                                    S3_PREFIX_TO_KEEP,
                                                                    dataset_version)

    pre_clean_publish_keys = publish_keys.union(publish_keys_to_keep)
    assert s3_keys(publish_bucket) == pre_clean_publish_keys
    assert s3_keys(embargo_bucket) == pre_clean_publish_keys
    assert s3_keys(asset_bucket) == asset_keys.union(asset_keys_to_keep)

    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE,
        'published_dataset_id': S3_PREFIX_TO_DELETE,
        'published_dataset_version': dataset_version,
        'publish_bucket': PUBLISH_BUCKET,
        'embargo_bucket': EMBARGO_BUCKET,
        'workflow_id': '5',
        'cleanup_stage': CleanupStageFailure
    }, {})

    assert s3_keys(publish_bucket) == publish_keys_to_keep
    assert s3_keys(embargo_bucket) == publish_keys_to_keep
    assert s3_keys(asset_bucket) == asset_keys_to_keep


def test_undo_copy_on_failure(publish_bucket, embargo_bucket, asset_bucket):
    dataset_id = S3_PREFIX_TO_DELETE
    dataset_version = 2

    created_keys = set()
    file_action_key = '{}/{}'.format(dataset_id, FileActionKey)
    created_keys.add(file_action_key)

    # a copied file with no version in the file action for publish bucket
    no_version_copied_key = '{}/{}/{}'.format(dataset_id, 'files', 'no-version-copied.txt')
    created_keys.add(no_version_copied_key)
    publish_bucket.upload_file(FILENAME, no_version_copied_key)
    embargo_bucket.upload_file(FILENAME, no_version_copied_key)

    # a copied file with a version in the file action for publish bucket
    copied_key = '{}/{}/{}'.format(dataset_id, 'files', 'copied.txt')
    created_keys.add(copied_key)

    # version 1 in the publish bucket
    publish_bucket.upload_file(FILENAME, copied_key)
    copied_v1 = publish_bucket.Object(copied_key).version_id

    # version 2 in the publish bucket
    publish_bucket.upload_file(FILENAME, copied_key)
    copied_v2 = publish_bucket.Object(copied_key).version_id

    assert copied_v1 != copied_v2

    embargo_bucket.upload_file(FILENAME, copied_key)

    embargo_bucket_file_actions = json.dumps({
        FileActionListTag: [
            {
                FileActionTag: FileActionCopy,
                FileActionBucketTag: embargo_bucket.name,
                FileActionPathTag: no_version_copied_key,
            },
            {
                FileActionTag: FileActionCopy,
                FileActionBucketTag: embargo_bucket.name,
                FileActionPathTag: copied_key,
            },
        ]
    })
    upload_content(embargo_bucket, embargo_bucket_file_actions, file_action_key)

    publish_bucket_file_actions = json.dumps({
        FileActionListTag: [
            {
                FileActionTag: FileActionCopy,
                FileActionBucketTag: publish_bucket.name,
                FileActionPathTag: no_version_copied_key,
            },
            {
                FileActionTag: FileActionCopy,
                FileActionBucketTag: publish_bucket.name,
                FileActionPathTag: copied_key,
                FileActionVersionTag: copied_v1
            },
        ]
    })
    upload_content(publish_bucket, publish_bucket_file_actions, file_action_key)

    assert s3_keys(publish_bucket) == created_keys
    assert s3_keys(embargo_bucket) == created_keys

    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE,
        'published_dataset_id': S3_PREFIX_TO_DELETE,
        'published_dataset_version': dataset_version,
        'publish_bucket': PUBLISH_BUCKET,
        'embargo_bucket': EMBARGO_BUCKET,
        'workflow_id': '5',
        'cleanup_stage': CleanupStageFailure
    }, {})

    assert s3_keys(embargo_bucket) == set()
    assert s3_keys(publish_bucket) == {copied_key}

    actual_versions = list(publish_bucket.object_versions.filter(Prefix=copied_key))
    assert len(actual_versions) == 1
    assert actual_versions[0].id == copied_v1


def setup_bucket(bucket_name, is_versioned):
    s3_resource.create_bucket(Bucket=bucket_name)
    bucket = s3_resource.Bucket(bucket_name)
    bucket.object_versions.all().delete()
    if is_versioned:
        s3_resource.BucketVersioning(bucket_name).enable()
    return bucket


def s3_keys(bucket):
    return {obj.key for obj in bucket.objects.all()}


def upload_content(bucket, content: str, key: str):
    file_like = io.BytesIO(content.encode('utf-8'))
    file_like.seek(0)
    bucket.upload_fileobj(Fileobj=file_like, Key=key)


def create_keys(prefix, filename, count=None):
    range_max = 1201 if count is None else count
    i = range(1, range_max)
    return list(map(lambda x: '{}/{}{}'.format(prefix, x, filename), i))


def assert_custom_bucket_request_contains_requester_pays(**kwargs):
    if kwargs['Bucket'] == PUBLISH_BUCKET or kwargs['Bucket'] == EMBARGO_BUCKET:
        assert kwargs.get('RequestPayer') == 'requester'
    else:
        assert kwargs.get('RequestPayer') is None


def create_publish_files(publish_bucket, embargo_bucket, asset_bucket, dataset_id, dataset_version,
                         include_intermediate_files=True) -> tuple[set[str], set[str]]:
    publish_keys, asset_keys = create_dataset_assets(publish_bucket,
                                                     embargo_bucket,
                                                     asset_bucket,
                                                     dataset_id,
                                                     dataset_version,
                                                     include_intermediate_files)

    graph_keys = create_graph_assets(publish_bucket, embargo_bucket, dataset_id, include_intermediate_files)

    keys = publish_keys.union(graph_keys)

    if include_intermediate_files:
        for name in PublishingIntermediateFiles:
            # these were already created by the functions above if include_intermediate_files == True
            if name != DatasetAssetsKey and name != GraphAssetsKey:
                key = '{}/{}'.format(dataset_id, name)
                keys.add(key)
                if name == FileActionKey or name == MetadataCleanupKey or name == RevisionsCleanupKey:
                    # these files are read in the failure stage so we send empty JSON to avoid an error
                    upload_content(publish_bucket, '{}', key)
                    upload_content(embargo_bucket, '{}', key)
                else:
                    publish_bucket.upload_file(FILENAME, key)
                    embargo_bucket.upload_file(FILENAME, key)

    return keys, asset_keys


def create_dataset_assets(publish_bucket, embargo_bucket, asset_bucket, dataset_id, dataset_version,
                          include_intermediate_files=True) -> tuple[set[str], set[str]]:
    bucket_keys = set()
    asset_bucket_keys = set()

    asset_names = ['banner.jpg', 'readme.md', 'changelog.md']
    asset_publish_keys = []

    for name in asset_names:
        key, dataset_asset_key = create_dataset_asset(publish_bucket, embargo_bucket, asset_bucket, dataset_id,
                                                      dataset_version, name)
        bucket_keys.add(key)
        asset_bucket_keys.add(dataset_asset_key)
        asset_publish_keys.append(key)

    if include_intermediate_files:
        # Populate buckets with a publish.json file that contains info on dataset assets
        dataset_assets_file_key = '{}/{}'.format(dataset_id, DatasetAssetsKey)
        bucket_keys.add(dataset_assets_file_key)

        banner_manifest = {'path': asset_names[0]}
        readme_manifest = {'path': asset_names[1]}
        changelog_manifest = {'path': asset_names[2]}

        # embargo version has no S3 version ids
        embargo_dataset_assets = json.dumps({
            'bannerManifest': banner_manifest,
            'readmeManifest': readme_manifest,
            'changelogManifest': changelog_manifest
        })
        upload_content(embargo_bucket, embargo_dataset_assets, dataset_assets_file_key)

        # now add version ids for publish bucket
        banner_obj = publish_bucket.Object(asset_publish_keys[0])
        banner_manifest['s3VersionId'] = banner_obj.version_id
        readme_obj = publish_bucket.Object(asset_publish_keys[1])
        readme_manifest['s3VersionId'] = readme_obj.version_id
        changelog_obj = publish_bucket.Object(asset_publish_keys[2])
        changelog_manifest['s3VersionId'] = changelog_obj.version_id

        dataset_assets = json.dumps({
            'bannerManifest': banner_manifest,
            'readmeManifest': readme_manifest,
            'changelogManifest': changelog_manifest
        })

        upload_content(publish_bucket, dataset_assets, dataset_assets_file_key)

    return bucket_keys, asset_bucket_keys


def create_graph_assets(publish_bucket, embargo_bucket, dataset_id, include_intermediate_files=True) -> set[str]:
    publish_keys = set()

    asset_names = ['schema.csv', 'models.csv', 'records.csv', 'relationships.csv']
    asset_publish_keys = []

    for name in asset_names:
        key = '{}/{}/{}'.format(dataset_id, MetadataPrefix, name)

        publish_bucket.upload_file(FILENAME, key)
        embargo_bucket.upload_file(FILENAME, key)

        publish_keys.add(key)
        asset_publish_keys.append(key)

    if include_intermediate_files:
        # Populate buckets with a graph.json file that contains info on graph assets
        graph_assets_file_key = '{}/{}'.format(dataset_id, GraphAssetsKey)

        manifests = []

        # embargo version wouldn't have S3 version ids
        for (name, key) in zip(asset_names, asset_publish_keys):
            manifests.append({
                'path': '{}/{}'.format(MetadataPrefix, name),
            })

        embargo_graph_assets = json.dumps({
            'manifests': manifests
        })

        upload_content(embargo_bucket, embargo_graph_assets, graph_assets_file_key)

        # now add the version ids for the publish bucket version
        for (key, manifest) in zip(asset_publish_keys, manifests):
            obj = publish_bucket.Object(key)
            manifest['s3VersionId'] = obj.version_id

        graph_assets = json.dumps({
            'manifests': manifests
        })

        upload_content(publish_bucket, graph_assets, graph_assets_file_key)
        publish_keys.add(graph_assets_file_key)

    return publish_keys


def create_dataset_asset(publish_bucket, embargo_bucket, asset_bucket, dataset_id, dataset_version, asset_name) -> \
        tuple[str, str]:
    # the key in the publish and embargo buckets
    asset_key = '{}/{}'.format(dataset_id, asset_name)
    # the key in the dataset asset bucket includes the version since this bucket is not versioned.
    asset_bucket_key = '{}/{}/{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, dataset_id, dataset_version, asset_name)

    publish_bucket.upload_file(FILENAME, asset_key)
    embargo_bucket.upload_file(FILENAME, asset_key)
    asset_bucket.upload_file(FILENAME, asset_bucket_key)

    return asset_key, asset_bucket_key


class MockClient:
    @staticmethod
    def delete_objects(**kwargs):
        assert_custom_bucket_request_contains_requester_pays(**kwargs)


class MockPaginator:
    @staticmethod
    def paginate(**kwargs):
        assert_custom_bucket_request_contains_requester_pays(**kwargs)
        prefix = kwargs['Prefix']
        page_size = kwargs['PaginationConfig']['PageSize']
        keys = create_keys(prefix, FILENAME)
        for i in range(0, len(keys), page_size):
            key_maps = [{'Key': k} for k in keys[i:i + page_size]]
            yield dict(Contents=key_maps)
