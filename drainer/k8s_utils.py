import logging
import pprint
import time

from kubernetes.client.rest import ApiException
from kubernetes.client.models import V1ConfigMap

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

MIRROR_POD_ANNOTATION_KEY = "kubernetes.io/config.mirror"
CONTROLLER_KIND_DAEMON_SET = "DaemonSet"
INSTANCE_CONFIGMAP_NAMESPACE = 'kube-system'
INSTANCE_CONFIGMAP_NAME = 'aws-instances'
MINUTES_FOR_EVICTION = 2


def create_instance_configmap(api):
    """Creates the instance configmap if it does not exist,
    other cases like unauthorized or server error will throw an exception"""
    try:
        api.read_namespaced_config_map(
            name=INSTANCE_CONFIGMAP_NAME,
            namespace=INSTANCE_CONFIGMAP_NAMESPACE,
            exact=True,
            export=True,
        )
    except ApiException as err:
        # if we get a 404 status, the configmap was not found
        if err.status == 404:
            try:
                cm = V1ConfigMap(
                    api_version='v1',
                    kind='ConfigMap',
                    metadata={'name': INSTANCE_CONFIGMAP_NAME, 'namespace': INSTANCE_CONFIGMAP_NAMESPACE},
                )
                api.create_namespaced_config_map(namespace=INSTANCE_CONFIGMAP_NAMESPACE, body=cm)
            except ApiException as create_err:
                # need to check configmap already exists in case this gets triggered multiple times
                # locking should be handled by the kubernetes apiserver?
                if create_err.reason == 'AlreadyExists':
                    return
                logger.exception('Failed to create instance configmap: {}'.format(create_err))
        else:
            logger.exception('Failed to retrieve instance configmap: {}'.format(err))


def get_instance_node_name_from_configmap(api, instance_id):
    """Gets the node name associated with the instance id in the instance configmap
    rather than throwing an exception, it returns None"""
    logger.info('Getting node name mapped to instance id {} from instance configmap'.format(instance_id))
    try:
        cm = api.read_namespaced_config_map(
            name=INSTANCE_CONFIGMAP_NAME,
            namespace=INSTANCE_CONFIGMAP_NAMESPACE,
            exact=True,
            export=True,
        )
        if not cm.data:
            return None
        return cm.data.get(instance_id, None)
    except ApiException:
        return None


def update_instance_in_configmap(api, instance_id, node_name):
    """Add/update an instance id to node_name mapping in the instance configmap
    this will be used for node deletion without having to make an api call to describe an instance and help with
    nodes that are terminated outside a scale in event in the ASG"""
    logger.info('Updating instance configmap with the mapping: {} = {}'.format(instance_id, node_name))
    try:
        cm = api.read_namespaced_config_map(
            name=INSTANCE_CONFIGMAP_NAME,
            namespace=INSTANCE_CONFIGMAP_NAMESPACE,
            exact=True,
            export=True,
        )
        if not cm.data:
            cm.data = dict()
        cm.data[instance_id] = node_name
        api.patch_namespaced_config_map(name=INSTANCE_CONFIGMAP_NAME, namespace=INSTANCE_CONFIGMAP_NAMESPACE, body=cm)
    except ApiException as err:
        logger.exception('Failed to update instance configmap: {}'.format(err))


def remove_instance_from_configmap(api, instance_id):
    """Removes an instance id to node_name mapping from the instance configmap and to be ran
    after a node is deleted from kubernetes"""
    logger.info('Removing instance {} from instance configmap'.format(instance_id))
    try:
        cm = api.read_namespaced_config_map(
            name=INSTANCE_CONFIGMAP_NAME,
            namespace=INSTANCE_CONFIGMAP_NAMESPACE,
            exact=True,
            export=True,
        )
        cm.data[instance_id] = None
        api.patch_namespaced_config_map(name=INSTANCE_CONFIGMAP_NAME, namespace=INSTANCE_CONFIGMAP_NAMESPACE, body=cm)
    except ApiException as err:
        logger.exception('Failed to update instance configmap: {}'.format(err))


def delete_node(api, node_name):
    """Deletes a node from the Kubernetes cluster"""
    logger.info('Deleting node {} from the cluster'.format(node_name))
    try:
        api.delete_node(node_name)
        logger.info('Deleted node {}'.format(node_name))
    except ApiException as err:
        logger.exception('Failed to delete node {}: {}'.format(node_name, err))


def delete_rook_ceph_crashcollector(apps_api, rook_ceph_crashcollectors_namespace, node_name):
    """Delete rook ceph crashcollector deployment and underlying replicaset corresponding to node"""
    logger.info('Deleting rook ceph crashcollector on node {}'.format(node_name))
    try:
        logger.debug('Locating rook ceph crashcollector deployment corresponding to node {}'.format(node_name))
        crashcollector_deployments = apps_api.list_namespaced_deployment(
            namespace=rook_ceph_crashcollectors_namespace,
            label_selector='app=rook-ceph-crashcollector,node_name={}'.format(node_name),
            include_uninitialized=True,
        )
        for dep in crashcollector_deployments.items:
            logger.info('Deleting deployment {}/{}'.format(rook_ceph_crashcollectors_namespace, dep.metadata.name))
            apps_api.delete_namespaced_deployment(
                name=dep.metadata.name,
                namespace=rook_ceph_crashcollectors_namespace,
            )
            logger.info('Deleted deployment {}/{}'.format(rook_ceph_crashcollectors_namespace, dep.metadata.name))

        logger.debug('Locating rook ceph crashcollector replicaset corresponding to node {}'.format(node_name))
        crashcollector_replicasets = apps_api.list_namespaced_replica_set(
            namespace=rook_ceph_crashcollectors_namespace,
            label_selector='app=rook-ceph-crashcollector,node_name={}'.format(node_name),
            include_uninitialized=True,
        )
        for rs in crashcollector_replicasets.items:
            logger.info('Deleting replicaset {}/{}'.format(rook_ceph_crashcollectors_namespace, rs.metadata.name))
            apps_api.delete_namespaced_replica_set(
                name=rs.metadata.name,
                namespace=rook_ceph_crashcollectors_namespace,
            )
            logger.info('Deleted replicaset {}/{}'.format(rook_ceph_crashcollectors_namespace, rs.metadata.name))
    except ApiException as err:
        logger.exception('Failed to delete rook ceph crashcollector on node {}: {}'.format(node_name, err))


def detach_node_rook_volumes(custom_obj_api, rook_ceph_volumes_namespace, node_name):
    """Detaches the rook volumes (apiVersion: rook.io/v1alpha2, Kind: Volume) that are attached to the node"""
    logger.debug('Getting list of rook volumes')
    rook_volumes = custom_obj_api.list_namespaced_custom_object(
        group='rook.io',
        version='v1alpha2',
        namespace=rook_ceph_volumes_namespace,
        plural='volumes',
        watch=False,
    )

    logger.debug(pprint.pformat(rook_volumes))

    for rook_volume in rook_volumes['items']:
        incides_to_remove = []

        # find which attachments in the rook volume belong to the node being terminated
        for i, attachment in enumerate(rook_volume['attachments']):
            if attachment['node'] == node_name:
                incides_to_remove.append(i)

        if incides_to_remove:
            incides_to_remove.reverse()

            for i in incides_to_remove:
                del rook_volume['attachments'][i]

            # patch the rook volume
            logger.info('Detaching node {} from rook volume {}'.format(node_name, rook_volume['metadata']['name']))
            try:
                custom_obj_api.patch_namespaced_custom_object(
                    group='rook.io',
                    version='v1alpha2',
                    namespace=rook_ceph_volumes_namespace,
                    plural='volumes',
                    name=rook_volume['metadata']['name'],
                    body=rook_volume,
                )
                logger.debug(pprint.pformat(rook_volume))
                logger.info('Detached node {} from rook volume {}'.format(node_name, rook_volume['metadata']['name']))
            except ApiException as err:
                logger.exception('Failed to detach node {} from rook volume {}: {}'.format(
                    node_name, rook_volume['metadata']['name'], err))


def cordon_node(api, node_name):
    """Marks the specified node as unschedulable, which means that no new pods can be launched on the
    node by the Kubernetes scheduler.
    """
    patch_body = {
        'apiVersion': 'v1',
        'kind': 'Node',
        'metadata': {
            'name': node_name
        },
        'spec': {
            'unschedulable': True
        }
    }

    api.patch_node(node_name, patch_body)


def remove_all_pods(api, node_name, poll=5, pod_eviction_timeout=120, pod_delete_grace_period=None):
    """Removes all Kubernetes pods from the specified node."""
    pods = get_evictable_pods(api, node_name)

    logger.debug('Number of pods to delete: ' + str(len(pods)))

    evict_until_completed(api, pods, poll)
    wait_until_empty(api, node_name, poll, pod_eviction_timeout, pod_delete_grace_period)


def pod_is_evictable(pod):
    if pod.metadata.annotations is not None and pod.metadata.annotations.get(MIRROR_POD_ANNOTATION_KEY):
        logger.info("Skipping mirror pod {}/{}".format(pod.metadata.namespace, pod.metadata.name))
        return False
    if pod.metadata.owner_references is None:
        return True
    for ref in pod.metadata.owner_references:
        if ref.controller is not None and ref.controller:
            if ref.kind == CONTROLLER_KIND_DAEMON_SET:
                logger.info("Skipping DaemonSet {}/{}".format(pod.metadata.namespace, pod.metadata.name))
                return False
    return True


def get_evictable_pods(api, node_name):
    field_selector = 'spec.nodeName=' + node_name
    pods = api.list_pod_for_all_namespaces(watch=False, field_selector=field_selector, include_uninitialized=True)
    return [pod for pod in pods.items if pod_is_evictable(pod)]


def evict_until_completed(api, pods, poll):
    pending = pods
    while True:
        pending = evict_pods(api, pending)
        if (len(pending)) <= 0:
            return
        time.sleep(poll)


def evict_pods(api, pods):
    remaining = []
    for pod in pods:
        logger.info('Evicting pod {} in namespace {}'.format(pod.metadata.name, pod.metadata.namespace))
        body = {
            'apiVersion': 'policy/v1beta1',
            'kind': 'Eviction',
            'deleteOptions': {},
            'metadata': {
                'name': pod.metadata.name,
                'namespace': pod.metadata.namespace
            }
        }
        try:
            api.create_namespaced_pod_eviction(pod.metadata.name + '-eviction', pod.metadata.namespace, body)
        except ApiException as err:
            if err.status == 429:
                remaining.append(pod)
                logger.warning("Pod {}/{} could not be evicted due to disruption budget. Will retry.".format(
                    pod.metadata.namespace, pod.metadata.name))
            else:
                logger.exception("Unexpected error adding eviction for pod {}/{}".format(
                    pod.metadata.namespace, pod.metadata.name))
        except:
            logger.exception("Unexpected error adding eviction for pod {}/{}".format(
                pod.metadata.namespace, pod.metadata.name))
    return remaining


def wait_until_empty(api, node_name, poll, pod_eviction_timeout=120, pod_delete_grace_period=None):
    logger.info("Waiting for evictions to complete")
    timeout = time.time() + pod_eviction_timeout
    while time.time() < timeout:
        pods = get_evictable_pods(api, node_name)
        if len(pods) <= 0:
            logger.info("All pods evicted successfully")
            return
        logger.debug("Still waiting for deletion of the following pods: {}".format(
            ", ".join(map(lambda pod: pod.metadata.namespace + "/" + pod.metadata.name, pods))))
        time.sleep(poll)
    if pod_delete_grace_period:
        pods = get_evictable_pods(api, node_name)
        try:
            for pod in pods:
                logger.info('Deleting pod {} in namespace {}'.format(pod.metadata.name, pod.metadata.namespace))
                api.delete_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    propagation_policy='Foreground',
                    grace_period=pod_delete_grace_period,
                )
                logger.info('Deleted pod {} in namespace {}'.format(pod.metadata.name, pod.metadata.namespace))
                time.sleep(1)
        except ApiException as err:
            logger.exception('Failed to evict pods on node {}: {}'.format(node_name, err))


def node_exists(api, node_name):
    """Determines whether the specified node is still part of the cluster."""
    nodes = api.list_node(include_uninitialized=True, pretty=True).items
    node = next((n for n in nodes if n.metadata.name == node_name), None)
    return False if not node else True


def abandon_lifecycle_action(asg_client, auto_scaling_group_name, lifecycle_hook_name, instance_id):
    """Completes the lifecycle action with the ABANDON result, which stops any remaining actions,
    such as other lifecycle hooks.
    """
    asg_client.complete_lifecycle_action(LifecycleHookName=lifecycle_hook_name,
                                         AutoScalingGroupName=auto_scaling_group_name,
                                         LifecycleActionResult='ABANDON',
                                         InstanceId=instance_id)
