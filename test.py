import boto3
import os
import pytest
import time
from main import lambda_handler, S3_URL

PUBLISH_BUCKET = 'test-discover-publish'
EMBARGO_BUCKET = 'test-discover-embargo'
ASSET_BUCKET = 'test-discover-assets'
DATASET_ASSETS_KEY_PREFIX = 'dataset-assets'

# This key corresponds to assets belonging to a dataset version
# that has either been unpublished or was not published successfully
S3_PREFIX_TO_DELETE = '1/10'

# This key corresponds to assets belonging to a dataset version
# that should remain untouched by this lambda function
S3_PREFIX_TO_KEEP = '1/100'

# This is a dummy file
FILENAME = 'test.txt'

s3_resource = boto3.resource('s3', endpoint_url=S3_URL)


@pytest.fixture(scope='module')
def setup():
    os.environ.update({
        'EMBARGO_BUCKET': EMBARGO_BUCKET,
        'ASSET_BUCKET': ASSET_BUCKET,
        'DATASET_ASSETS_KEY_PREFIX': DATASET_ASSETS_KEY_PREFIX
    })

    time.sleep(5)  # let localstack spin up


@pytest.fixture(scope='function')
def publish_bucket(setup):
    return setup_bucket(PUBLISH_BUCKET)


@pytest.fixture(scope='function')
def embargo_bucket(setup):
    return setup_bucket(EMBARGO_BUCKET)


@pytest.fixture(scope='function')
def asset_bucket(setup):
    return setup_bucket(ASSET_BUCKET)


def test_empty_dataset(publish_bucket, embargo_bucket, asset_bucket):
    s3_key_to_keep = '{}/{}'.format(S3_PREFIX_TO_KEEP, FILENAME)
    asset_key_to_keep = '{}/{}'.format(DATASET_ASSETS_KEY_PREFIX, s3_key_to_keep)

    publish_bucket.upload_file(Filename=FILENAME, Key=s3_key_to_keep)
    embargo_bucket.upload_file(Filename=FILENAME, Key=s3_key_to_keep)
    asset_bucket.upload_file(Filename=FILENAME, Key=asset_key_to_keep)

    assert s3_keys(publish_bucket) == [s3_key_to_keep]
    assert s3_keys(embargo_bucket) == [s3_key_to_keep]
    assert s3_keys(asset_bucket) == [asset_key_to_keep]

    # RUN LAMBDA
    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE,
        's3_bucket': PUBLISH_BUCKET
    }, {})

    # VERIFY RESULTS
    assert s3_keys(publish_bucket) == [s3_key_to_keep]
    assert s3_keys(embargo_bucket) == [s3_key_to_keep]
    assert s3_keys(asset_bucket) == [asset_key_to_keep]


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
    assert sorted(s3_keys(publish_bucket)) == sorted(expected_keys)
    assert sorted(s3_keys(embargo_bucket)) == sorted(expected_keys)
    assert s3_keys(asset_bucket) == [asset_key_to_delete, asset_key_to_keep]

    # RUN LAMBDA
    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE,
        's3_bucket': PUBLISH_BUCKET
    }, {})

    # VERIFY RESULTS
    assert s3_keys(publish_bucket) == [s3_key_to_keep]
    assert s3_keys(embargo_bucket) == [s3_key_to_keep]
    assert s3_keys(asset_bucket) == [asset_key_to_keep]


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

    expected_keys = [s3_key_to_delete, s3_key_to_keep]
    assert s3_keys(publish_bucket) == expected_keys
    assert s3_keys(embargo_bucket) == expected_keys
    assert s3_keys(asset_bucket) == [asset_key_to_delete, asset_key_to_keep]

    # RUN LAMBDA
    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE + "/",
        's3_bucket': PUBLISH_BUCKET
    }, {})

    # VERIFY RESULTS
    assert s3_keys(publish_bucket) == [s3_key_to_keep]
    assert s3_keys(embargo_bucket) == [s3_key_to_keep]
    assert s3_keys(asset_bucket) == [asset_key_to_keep]


def test_include_requestor_pays():
    lambda_handler({
        's3_key_prefix': S3_PREFIX_TO_DELETE,
        's3_bucket': PUBLISH_BUCKET
    }, {}, s3_client=MockClient(), s3_paginator=MockPaginator())


def setup_bucket(bucket_name):
    s3_resource.create_bucket(Bucket=bucket_name)
    bucket = s3_resource.Bucket(bucket_name)
    bucket.objects.all().delete()
    return bucket


def s3_keys(bucket):
    return [obj.key for obj in bucket.objects.all()]


def create_keys(prefix, filename):
    i = range(1, 1201)
    return list(map(lambda x: '{}/{}{}'.format(prefix, x, filename), i))


def assert_publish_bucket_request_contains_requester_pays(**kwargs):
    if kwargs['Bucket'] == PUBLISH_BUCKET:
        assert kwargs.get('RequestPayer') == 'requester'
    else:
        assert kwargs.get('RequestPayer') is None


class MockClient:
    @staticmethod
    def delete_objects(**kwargs):
        assert_publish_bucket_request_contains_requester_pays(**kwargs)


class MockPaginator:
    @staticmethod
    def paginate(**kwargs):
        assert_publish_bucket_request_contains_requester_pays(**kwargs)
        prefix = kwargs['Prefix']
        page_size = kwargs['PaginationConfig']['PageSize']
        keys = create_keys(prefix, FILENAME)
        for i in range(0, len(keys), page_size):
            key_maps = [{'Key': k} for k in keys[i:i+page_size]]
            yield dict(Contents=key_maps)
