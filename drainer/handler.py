import boto3
import base64
import json
import logging
import os.path
import re
import yaml

from botocore.signers import RequestSigner
import kubernetes as k8s
from kubernetes.client.rest import ApiException

import k8s_utils


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

KUBE_FILEPATH = '/tmp/kubeconfig'
REGION = os.environ['AWS_REGION']
ASG_ACTION = {
    'launch': 'EC2 Instance-launch Lifecycle Action',
    'terminate': 'EC2 Instance-terminate Lifecycle Action',
}

eks = boto3.client('eks', region_name=REGION)
ec2 = boto3.client('ec2', region_name=REGION)
asg = boto3.client('autoscaling', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)


def create_kube_config(eks, cluster_name):
    """Creates the Kubernetes config file required when instantiating the API client."""
    cluster_info = eks.describe_cluster(name=cluster_name)['cluster']
    certificate = cluster_info['certificateAuthority']['data']
    endpoint = cluster_info['endpoint']

    kube_config = {
        'apiVersion': 'v1',
        'clusters': [
            {
                'cluster':
                    {
                        'server': endpoint,
                        'certificate-authority-data': certificate
                    },
                'name': 'k8s'

            }],
        'contexts': [
            {
                'context':
                    {
                        'cluster': 'k8s',
                        'user': 'aws'
                    },
                'name': 'aws'
            }],
        'current-context': 'aws',
        'Kind': 'config',
        'users': [
            {
                'name': 'aws',
                'user': 'lambda'
            }]
    }

    with open(KUBE_FILEPATH, 'w') as f:
        yaml.dump(kube_config, f, default_flow_style=False)


def get_bearer_token(cluster, region):
    """Creates the authentication to token required by AWS IAM Authenticator. This is
    done by creating a base64 encoded string which represents a HTTP call to the STS
    GetCallerIdentity Query Request (https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html).
    The AWS IAM Authenticator decodes the base64 string and makes the request on behalf of the user.
    """
    STS_TOKEN_EXPIRES_IN = 60
    session = boto3.session.Session()

    client = session.client('sts', region_name=region)
    service_id = client.meta.service_model.service_id

    signer = RequestSigner(
        service_id,
        region,
        'sts',
        'v4',
        session.get_credentials(),
        session.events
    )

    params = {
        'method': 'GET',
        'url': 'https://sts.{}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15'.format(region),
        'body': {},
        'headers': {
            'x-k8s-aws-id': cluster
        },
        'context': {}
    }

    signed_url = signer.generate_presigned_url(
        params,
        region_name=region,
        expires_in=STS_TOKEN_EXPIRES_IN,
        operation_name=''
    )

    base64_url = base64.urlsafe_b64encode(signed_url.encode('utf-8')).decode('utf-8')

    # need to remove base64 encoding padding:
    # https://github.com/kubernetes-sigs/aws-iam-authenticator/issues/202
    return 'k8s-aws-v1.' + re.sub(r'=*', '', base64_url)


def get_node_name_from_cloudtrail_events(instance_id, region):
    """tries to retrieve the node name from cloudtrail events and requires cloudtrail:LookupEvents permissions
    this usually is needed if the instance is terminated outside of a scaling in event of the autoscaling group
    NOTE: cloudtrail only keeps logs for 90 days, so if the node was created after that you will not get the name"""
    session = boto3.session.Session()
    client = session.client('cloudtrail', region_name=region)
    events = client.lookup_events(
        LookupAttributes=[
            {
                'AttributeKey': 'Username',
                'AttributeValue': instance_id,
            },
            {
                'AttributeKey': 'EventName',
                'AttributeValue': 'CreateNetworkInterface',
            },            {
                'AttributeKey': 'EventSource',
                'AttributeValue': 'ec2.amazonaws.com',
            },
        ],
        MaxResults=1,
    )['Events']

    # should only be 1 event returned if non-empty
    if not events or 'CloudTrailEvent' not in events[0] or not events[0]['CloudTrailEvent']:
        return ""

    event = json.loads(events[0]['CloudTrailEvent'])
    return event['responseElements']['networkInterface']['privateDnsName']


def get_node_name_from_instance_id(api, cluster_name, instance_id, skip_configmap=False):
    node_name = None

    if skip_configmap:
        logger.debug('Getting node name from instance configmap')
        node_name = k8s_utils.get_instance_node_name_from_configmap(api, instance_id)

    if not node_name:
        logger.debug('Instance configmap did not return anything, will try and get via describe instances')
        instance = ec2.describe_instances(
            InstanceIds=[instance_id],
            Filters=[{'Name': 'tag-key', 'Values': ['kubernetes.io/cluster/{}'.format(cluster_name)]}],
        )

        try:
            node_name = instance['Reservations'][0]['Instances'][0]['PrivateDnsName']
        except:
            pass

        if not node_name:
            logger.debug('Did not find node name from instance description, will try and get frrom cloudtrail events')
            node_name = get_node_name_from_cloudtrail_events(instance_id, REGION)

    return node_name


def _lambda_handler(env, k8s_config, k8s_client, event):
    kube_config_bucket = env['kube_config_bucket']
    cluster_name = env['cluster_name']

    if not os.path.exists(KUBE_FILEPATH):
        if kube_config_bucket:
            logger.info('No kubeconfig file found. Downloading...')
            s3.download_file(kube_config_bucket, env['kube_config_object'], KUBE_FILEPATH)
        else:
            logger.info('No kubeconfig file found. Generating...')
            create_kube_config(eks, cluster_name)

    detail_type = event['detail-type']
    logger.info('Event Type: ' + detail_type)
    lifecycle_hook_name = event['detail']['LifecycleHookName']
    logger.info('Lifecycle Hook: ' + lifecycle_hook_name)
    auto_scaling_group_name = event['detail']['AutoScalingGroupName']
    instance_id = event['detail']['EC2InstanceId']
    logger.info('Instance ID: ' + instance_id)

    # Configure
    k8s_config.load_kube_config(KUBE_FILEPATH)
    configuration = k8s_client.Configuration()
    if not kube_config_bucket:
        configuration.api_key['authorization'] = get_bearer_token(cluster_name, REGION)
        configuration.api_key_prefix['authorization'] = 'Bearer'
    # API
    api = k8s_client.ApiClient(configuration)
    v1 = k8s_client.CoreV1Api(api)
    apps_v1 = k8s_client.AppsV1Api(api)
    custom_obj_api = k8s_client.CustomObjectsApi(api)

    if detail_type == ASG_ACTION['launch']:
        logger.debug('Processing launch event...')

        # we don't want the update of the instances configmap to block the autoscaling group lifecycle as it's a
        # housekeeping task
        asg.complete_lifecycle_action(LifecycleHookName=lifecycle_hook_name,
                                      AutoScalingGroupName=auto_scaling_group_name,
                                      LifecycleActionResult='CONTINUE',
                                      InstanceId=instance_id)

        try:
            node_name = get_node_name_from_instance_id(v1, cluster_name, instance_id, skip_configmap=True)
            logger.info('Node name: ' + node_name)

            k8s_utils.create_instance_configmap(v1)
            k8s_utils.update_instance_in_configmap(v1, instance_id, node_name)

        except ApiException:
            logger.exception('There was an error adding instance {} to the {}/{} configmap'.format(
                instance_id, k8s_utils.INSTANCE_CONFIGMAP_NAMESPACE, k8s_utils.INSTANCE_CONFIGMAP_NAME))
            k8s_utils.abandon_lifecycle_action(asg, auto_scaling_group_name, lifecycle_hook_name, instance_id)

    elif detail_type == ASG_ACTION['terminate']:
        logger.debug('Processing terminate event...')

        node_name = None

        try:
            node_name = get_node_name_from_instance_id(v1, cluster_name, instance_id)
            logger.info('Node name: ' + node_name)

            if not k8s_utils.node_exists(v1, node_name):
                logger.error('Node not found.')
                k8s_utils.abandon_lifecycle_action(asg, auto_scaling_group_name, lifecycle_hook_name, instance_id)
                return

            if env['detach_rook_volumes'].lower() == 'true' and env['rook_ceph_volumes_namespace']:
                k8s_utils.detach_node_rook_volumes(custom_obj_api, env['rook_ceph_volumes_namespace'], node_name)

            k8s_utils.cordon_node(v1, node_name)

            timeout = None if not env['pod_eviction_timeout'] else int(env['pod_eviction_timeout'])
            grace_period = None if not env['pod_delete_grace_period'] else int(env['pod_delete_grace_period'])
            k8s_utils.remove_all_pods(v1, node_name, pod_eviction_timeout=timeout, pod_delete_grace_period=grace_period)

            if env['delete_rook_ceph_crashcollector'].lower() == 'true':
                k8s_utils.delete_rook_ceph_crashcollector(
                    apps_v1, env['rook_ceph_crashcollectors_namespace'], node_name)

            if env['delete_node'].lower() == 'true':
                k8s_utils.delete_node(v1, node_name)

            asg.complete_lifecycle_action(LifecycleHookName=lifecycle_hook_name,
                                          AutoScalingGroupName=auto_scaling_group_name,
                                          LifecycleActionResult='CONTINUE',
                                          InstanceId=instance_id)
        except ApiException:
            if node_name:
                logger.exception('There was an error removing the pods from the node'.format(node_name))
            else:
                logger.exception('There was an error removing the pods from the instance {} node'.format(instance_id))
            k8s_utils.abandon_lifecycle_action(asg, auto_scaling_group_name, lifecycle_hook_name, instance_id)

        # we don't want the removal of the instance id to fail the completion of the autoscaling group lifecycle
        # since it's just a housekeeping task
        try:
            k8s_utils.remove_instance_from_configmap(v1, instance_id)
        except ApiException:
            logger.exception(
                'There was an error removing the instance {} from the instances confignmap'.format(instance_id))

    else:
        logger.debug('Processing event...')
        asg.complete_lifecycle_action(LifecycleHookName=lifecycle_hook_name,
                                      AutoScalingGroupName=auto_scaling_group_name,
                                      LifecycleActionResult='CONTINUE',
                                      InstanceId=instance_id)


def lambda_handler(event, _):
    env = {
        'cluster_name': os.environ.get('CLUSTER_NAME'),
        'kube_config_bucket': os.environ.get('KUBE_CONFIG_BUCKET'),
        'kube_config_object': os.environ.get('KUBE_CONFIG_OBJECT'),
        'pod_eviction_timeout': os.environ.get('POD_EVICTION_TIMEOUT', 60*2),
        'pod_delete_grace_period': os.environ.get('POD_DELETE_GRACE_PERIOD'),
        'rook_ceph_volumes_namespace': os.environ.get('ROOK_CEPH_VOLUMES_NAMESPACE', 'rook-ceph'),
        'detach_rook_volumes': os.environ.get('DETACH_ROOK_VOLUMES', 'true'),
        'rook_ceph_crashcollectors_namespace': os.environ.get('ROOK_CEPH_CRASHCOLLECTORS_NAMESPACE', 'rook-ceph'),
        'delete_rook_ceph_crashcollector': os.environ.get('DELETE_ROOK_CEPH_CRASHCOLLECTOR', 'true'),
        'delete_node': os.environ.get('DELETE_NODE', 'true'),
    }
    return _lambda_handler(env, k8s.config, k8s.client, event)
