import os
import boto3
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

        workflow_id = 4
        if "workflow_id" in event:
            workflow_id = int(event["workflow_id"])

        if workflow_id == 5:
            purge_v5()
        else:
            purge_v4(asset_bucket_id, assets_prefix, publish_bucket_id, embargo_bucket_id, event['s3_key_prefix'], s3_client, s3_paginator)

    except Exception as e:
        log.error(e, exc_info=True)
        raise


def purge_v5():
    # TODO: form well-known file name ➝ "Published Files List"
    # TODO: check whether "Published Files List" is present (on S3)
    # TODO: when "Published Files List" is present, open the file, read the content, and delete files listed in the file
    pass

def purge_v4(asset_bucket_id, assets_prefix, publish_bucket_id, embargo_bucket_id, s3_key_prefix_evt, s3_client, s3_paginator):
    try:
        # Ensure the S3 key ends with a '/'
        if s3_key_prefix_evt.endswith('/'):
            s3_key_prefix = s3_key_prefix_evt
        else:
            s3_key_prefix = '{}/'.format(s3_key_prefix_evt)

        assert s3_key_prefix.endswith('/')
        assert len(s3_key_prefix) > 1  # At least one character + slash

        dataset_assets_prefix = '{}/{}'.format(assets_prefix, s3_key_prefix)

        # Rebind Pennsieve log context with event info
        log = log.bind(
            pennsieve={
                'service_name': FULL_SERVICE_NAME,
                'publish_bucket': publish_bucket_id,
                'embargo_bucket': embargo_bucket_id,
                's3_key_prefix': s3_key_prefix
            },
        )

        log.info('Starting lambda')

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
