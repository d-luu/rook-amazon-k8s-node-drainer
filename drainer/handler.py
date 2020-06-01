import boto3
import base64
import json
import logging
import os.path
import re
import time
import yaml

from pprint import pprint

from botocore.signers import RequestSigner
import kubernetes as k8s
from kubernetes.client.rest import ApiException

import k8s_utils


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CLOUDWATCH_LOG_WINDOW_SECONDS = 600
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


# NOTE: cloudtrail doesn't always return the node name for the instance, it just returns one of the IPs
#       so we are not going to use it to grab the node name
#       may want to try AWS Config, but it may likely be the same


def get_node_name_from_kube_audit_logs(cluster_name, instance_id, region):
    """tries to retrieve the node name from the audit logs"""
    session = boto3.session.Session()
    client = session.client('logs', region_name=region)

    end_time = int(time.time())
    start_time = end_time - 86400  # node status log captured once a day
    query_str_fmt = ('filter @logStream like /kube-apiserver-audit/ | fields @message | '
                     'filter @message like /aws:\/\/\/.*\/{instance_id}/ | limit 1')

    try:
        logger.debug('Querying kube apiserver audit logs...')
        query = client.start_query(
            logGroupName='/aws/eks/{}/cluster'.format(cluster_name),
            startTime=start_time,
            endTime=end_time,
            # queries are returned in descending cloudwatch logs timestamp
            queryString=query_str_fmt.format(instance_id=instance_id),
            limit=1,
        )
        logger.debug('query id: {}'.format(query['queryId']))

        def query_node_name():
            results = client.get_query_results(queryId=query['queryId'])
            if results['status'] == 'Complete':
                for r in results['results']:
                    for f in r:
                        if f['field'] == '@message':
                            val = json.loads(f['value'])
                            if val['responseObject']['spec']['providerID'].endswith(instance_id):
                                return val['responseObject']['metadata']['name']
                return ''
            else:
                return None

        # check at least once
        # queries usually take at least 5 seconds, so no point in checking too soon
        time.sleep(10)
        name = query_node_name()
        # we differentiate between None (query not completed) and empty string (query completed, no result)
        if name:
            return name
        elif name is not None:
            return None

        # if we made it here, the query has not completed yet
        max_checks = 5
        while max_checks > 0:
            max_checks -= 1
            time.sleep(5)
            name = query_node_name()
            # we differentiate between None (query not completed) and empty string (query completed, no result)
            if name:
                return name
            elif name is not None:
                return None

        return None
    except Exception as e:
        logger.debug(e)
        return None


def get_recent_node_name_from_kube_controller_logs(cluster_name, region):
    """NOTE: if multiple instances are terminated about the same time, they will possibly
    all select the same single instance and we will miss processing the other instances"""
    session = boto3.session.Session()
    client = session.client('logs', region_name=region)

    end_time = int(time.time())
    start_time = end_time - CLOUDWATCH_LOG_WINDOW_SECONDS
    query_str = ('filter @logStream like /kube-controller-manager/ | fields @message | '
                 'filter @message like /Controller observed a Node deletion: / | limit 1')

    try:
        logger.debug('Querying kube controller logs...')
        query = client.start_query(
            logGroupName='/aws/eks/{}/cluster'.format(cluster_name),
            startTime=start_time,
            endTime=end_time,
            # queries are returned in descending cloudwatch logs timestamp
            queryString=query_str,
        )
        logger.debug('query id: {}'.format(query['queryId']))

        def query_node_name():
            results = client.get_query_results(queryId=query['queryId'])

            if results['status'] == 'Complete':
                for r in results['results']:
                    for f in r:
                        if f['field'] == '@message':
                            result = f['value'].partition('Controller observed a Node deletion: ')[2]
                            if result:
                                return result
                return ''
            else:
                return None

        # check at least once
        # queries usually take some time to complete, so no point in checking too soon
        time.sleep(10)
        name = query_node_name()
        # we differentiate between None (query not completed) and empty string (query completed, no result)
        if name:
            return name
        elif name is not None:
            return None

        # if we made it here, the query has not completed yet
        max_checks = 5
        while max_checks > 0:
            max_checks -= 1
            time.sleep(5)
            name = query_node_name()
            # we differentiate between None (query not completed) and empty string (query completed, no result)
            if name:
                return name
            elif name is not None:
                return None

        return None
    except Exception as e:
        logger.debug(e)
        return None


def get_node_name_from_instance_id(api, env, cluster_name, instance_id, retries=5):
    node_name = None

    for _ in range(retries):
        logger.info('Getting node name from nodes list')
        node_names = k8s_utils.get_instance_node_mapping(api)

        logger.debug(node_names)

        node_name = node_names.get(instance_id)

        if not node_name:
            logger.info('Did not find node name in nodes list, will try and get from describe instances')
            instance = ec2.describe_instances(
                InstanceIds=[instance_id],
                Filters=[{'Name': 'tag-key', 'Values': ['kubernetes.io/cluster/{}'.format(cluster_name)]}],
            )

            logger.debug(instance)

            try:
                node_name = instance['Reservations'][0]['Instances'][0]['PrivateDnsName']
            except:
                pass

        if not node_name:
            logger.info('Did not find node name from describe instances, '
                        'will try to get from cloudwatch kube apiserver audit logs '
                        'from the past day')
            try:
                node_name = get_node_name_from_kube_audit_logs(cluster_name, instance_id, REGION)
            except:
                pass

        if not node_name:
            logger.info('Did not find node name from cloudwatch kube apiserver audit logs, '
                         'will try to get from cloudwatch kube controller logs '
                         'from the past {} seconds'.format(CLOUDWATCH_LOG_WINDOW_SECONDS))
            try:
                node_name = get_recent_node_name_from_kube_controller_logs(cluster_name, REGION)
            except:
                pass

        if node_name:
            return node_name

        logger.info('Retrying to get node name in 10 seconds...')
        time.sleep(10)

    return None


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
    batch_v1 = k8s_client.BatchV1Api(api)
    custom_obj_api = k8s_client.CustomObjectsApi(api)

    node_name = get_node_name_from_instance_id(v1, env, cluster_name, instance_id)

    if not node_name:
        logger.info('Node name not found. Unable to drain node.')
        k8s_utils.abandon_lifecycle_action(asg, auto_scaling_group_name, lifecycle_hook_name, instance_id)
        return

    logger.info('Node name: ' + node_name)

    if detail_type == ASG_ACTION['terminate']:
        logger.info('Processing terminate event...')

        try:
            # if not k8s_utils.node_exists(v1, node_name):
            #     logger.error('Node not found.')

            k8s_utils.cordon_node(v1, node_name)

            timeout = None if not env['pod_eviction_timeout'] else int(env['pod_eviction_timeout'])
            grace_period = None if not env['pod_delete_grace_period'] else int(env['pod_delete_grace_period'])
            k8s_utils.remove_all_pods(v1, node_name, pod_eviction_timeout=timeout,
                                      pod_delete_grace_period=grace_period)

            if env['delete_node'].lower() == 'true':
                # we dont check status, because if it fails, we would just continue anyways
                k8s_utils.delete_node(v1, node_name)

            logger.info('Completing lifecycle action')
            asg.complete_lifecycle_action(LifecycleHookName=lifecycle_hook_name,
                                          AutoScalingGroupName=auto_scaling_group_name,
                                          LifecycleActionResult='CONTINUE',
                                          InstanceId=instance_id)
        except ApiException as e:
            # the node can finish terminating (node not found) while we run the operations above,
            # continue if we have more to process
            if (e.status != 404 and
                    ((env['detach_rook_volumes'].lower() == 'true' and env['rook_ceph_volumes_namespace']) or
                     (env['update_ceph_crushmap'].lower() == 'true') or
                     (env['delete_rook_ceph_crashcollector'].lower() == 'true'))):
                if node_name:
                    logger.exception('There was an error removing the pods from the node'.format(node_name))
                else:
                    logger.exception('There was an error removing the pods from the instance {} node'.format(instance_id))
                logger.info('Abandoning lifecycle action')
                k8s_utils.abandon_lifecycle_action(asg, auto_scaling_group_name, lifecycle_hook_name, instance_id)

        try:
            if env['detach_rook_volumes'].lower() == 'true' and env['rook_ceph_volumes_namespace']:
                k8s_utils.detach_node_rook_volumes(custom_obj_api, env['rook_ceph_volumes_namespace'], node_name)

            osd_ids = []
            if env['rook_ceph_osd_namespace']:
                osd_ids = k8s_utils.get_host_associated_osd_ids(apps_v1, node_name, env['rook_ceph_osd_namespace'])

            if (env['update_ceph_crushmap'].lower() == 'true' and
                    env['rook_ceph_osd_namespace'] and env['rook_ceph_operator_namespace']):
                # TODO: add retries if received 500 status (in fact, add to any stream/exec api)

                k8s_utils.remove_host_and_osd_from_ceph_crushmap(v1, node_name, osd_ids,
                                                                 env['rook_ceph_operator_namespace'])

            if env['rook_ceph_osd_namespace']:
                k8s_utils.delete_rook_ceph_osd_deployment(apps_v1, osd_ids, env['rook_ceph_osd_namespace'])
                k8s_utils.cleanup_rook_ceph_osd_status_configmaps(v1, node_name, env['rook_ceph_osd_namespace'])
                k8s_utils.cleanup_rook_ceph_osd_prepare_jobs(batch_v1, node_name, env['rook_ceph_osd_namespace'])

            if env['delete_rook_ceph_crashcollector'].lower() == 'true':
                k8s_utils.delete_rook_ceph_crashcollector(
                    apps_v1, env['rook_ceph_crashcollectors_namespace'], node_name)

            if env['rook_ceph_mon_namespace']:
                k8s_utils.remove_node_from_mon_endpoints_configmap_and_secret(v1, node_name, env['rook_ceph_mon_namespace'])

            if env['rook_ceph_mon_namespace']:
                # k8s_utils.scale_node_rook_ceph_mon_deployment(apps_v1, node_name, env['rook_ceph_mon_namespace'], 0)
                k8s_utils.delete_node_rook_ceph_mon_deployment(apps_v1, node_name, env['rook_ceph_mon_namespace'])

            # there is an issue with the crashcollector looking for nodes that are gone and
            # stopping rook ceph operator from continuing
            # if we reload the config, rook will refresh and continue
            # the simplest way to do so is to actually toggle the ceph version unsupported flag as it won't affect the
            # cluster unless you explicitly use unsupported ceph versions, otherwise you need to bounce the operator
            if env['reload_rook_cephcluster'].lower() == 'true' and env['rook_ceph_osd_namespace']:
               k8s_utils.toggle_rook_ceph_version_allow_unsupported_flag(custom_obj_api, env['rook_ceph_osd_namespace'])

            # crashcollector reconciler will get stuck in a loop if we dont bounce the operator pods
            #if env['rook_ceph_operator_namespace']:
            #    k8s_utils.delete_rook_ceph_operator_pods(v1, env['rook_ceph_operator_namespace'])

            if env['wait_for_rook_ceph_health_ok_retries'] and env['rook_ceph_operator_namespace']:
               k8s_utils.wait_for_rook_ceph_health_ok(v1, env['rook_ceph_operator_namespace'],
                                                      retries=int(env['wait_for_rook_ceph_health_ok_retries']))

        except ApiException:
            logger.exception('There was an error cleaning up rook resources on node {}'.format(node_name))
            k8s_utils.abandon_lifecycle_action(asg, auto_scaling_group_name, lifecycle_hook_name, instance_id)

        try:
            asg.complete_lifecycle_action(LifecycleHookName=lifecycle_hook_name,
                                          AutoScalingGroupName=auto_scaling_group_name,
                                          LifecycleActionResult='CONTINUE',
                                          InstanceId=instance_id)
        except:
            # if you terminate a EC2 instance outside of the ASG scaling,
            # then sometimes you will get "No active Lifecycle Action found" ?
            pass
    else:
        logger.info('No event to process, continuing...')
        try:
            asg.complete_lifecycle_action(LifecycleHookName=lifecycle_hook_name,
                                          AutoScalingGroupName=auto_scaling_group_name,
                                          LifecycleActionResult='CONTINUE',
                                          InstanceId=instance_id)
        except:
            # if you terminate a EC2 instance outside of the ASG scaling,
            # then sometimes you will get "No active Lifecycle Action found" ?
            pass


def lambda_handler(event, _):
    env = {
        'cluster_name': os.environ.get('CLUSTER_NAME'),
        'kube_config_bucket': os.environ.get('KUBE_CONFIG_BUCKET'),
        'kube_config_object': os.environ.get('KUBE_CONFIG_OBJECT'),
        'pod_eviction_timeout': os.environ.get('POD_EVICTION_TIMEOUT', 60*2),
        'pod_delete_grace_period': os.environ.get('POD_DELETE_GRACE_PERIOD'),
        'rook_ceph_operator_namespace': os.environ.get('ROOK_CEPH_OPERATOR_NAMESPACE', 'rook-ceph'),
        'rook_ceph_osd_namespace': os.environ.get('ROOK_CEPH_OSD_NAMESPACE', 'rook-ceph'),
        'rook_ceph_mon_namespace': os.environ.get('ROOK_CEPH_MON_NAMESPACE', 'rook-ceph'),
        'rook_ceph_volumes_namespace': os.environ.get('ROOK_CEPH_VOLUMES_NAMESPACE', 'rook-ceph'),
        'rook_ceph_crashcollectors_namespace': os.environ.get('ROOK_CEPH_CRASHCOLLECTORS_NAMESPACE', 'rook-ceph'),
        'detach_rook_volumes': os.environ.get('DETACH_ROOK_VOLUMES', 'true'),
        'update_ceph_crushmap': os.environ.get('UPDATE_CEPH_CRUSHMAP', 'true'),
        'delete_rook_ceph_crashcollector': os.environ.get('DELETE_ROOK_CEPH_CRASHCOLLECTOR', 'true'),
        'delete_node': os.environ.get('DELETE_NODE', 'true'),
        'reload_rook_cephcluster': os.environ.get('RELOAD_ROOK_CEPH_CLUSTER', 'true'),
        'wait_for_rook_ceph_health_ok_retries': os.environ.get('WAIT_FOR_ROOK_CEPH_HEALTH_OK_RETRIES', 0),
    }
    return _lambda_handler(env, k8s.config, k8s.client, event)
