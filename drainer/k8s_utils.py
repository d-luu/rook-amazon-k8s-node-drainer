import base64
import binascii
import json
import logging
import pprint
import time
import yaml

from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

MIRROR_POD_ANNOTATION_KEY = "kubernetes.io/config.mirror"
CONTROLLER_KIND_DAEMON_SET = "DaemonSet"
INSTANCE_CONFIGMAP_NAMESPACE = 'kube-system'
INSTANCE_CONFIGMAP_NAME = 'aws-instances'
MINUTES_FOR_EVICTION = 2

LIST_ITEM_DELIMITER = ","

# named vars: osd_host, osd_ids_str
SCRIPT_COMPLETED_STR = "[INFO] completed"
CRUSHMAP_UPDATE_SCRIPT_STR_FMT = """
#!/bin/bash

# -------------
# this script assumes it's ran in the rook ceph operator pod
#
# NOTE: 
# we dont filter by active OSDs and hosts since we can get into a race condition when multiple nodes are brought down
# instead we pass in the OSDs and hosts to be removed
# -------------

# variables
OSD_HOST='{osd_host}'
OSD_IDS_STR='{osd_ids_str}'
SCRIPT_COMPLETED_STR='{script_completed_str}'
LIST_ITEM_DELIMITER='{list_item_delimiter}'

# process
IFS="${{LIST_ITEM_DELIMITER}}" read -ra OSD_IDS <<< "${{OSD_IDS_STR}}"
for id in "${{OSD_IDS[@]}}"; do

  echo "[INFO] taking out osd.${{id}}..."
  if ! (ceph osd out osd.${{id}}); then
    echo "[WARN] failed to take out osd.${{id}}"
    exit 1
  fi
  echo "[INFO] osd.${{id}} out"

  echo "[INFO] purging osd.${{id}}..."
  if ! (ceph osd purge ${{id}} --yes-i-really-mean-it); then
    echo "[WARN] failed to purge osd ${{id}}"
    exit 1
  fi
  echo "[INFO] osd ${{id}} purged"
  
  echo "[INFO] waiting for a brief moment before checking ceph status..."
  sleep 5
  
  # wait for ceph to be healthy and PGs to all be active+clean
  echo "[INFO] waiting for ceph to be healthy and PGs to all be active+clean..."
  RETRIES=15
  while [ ${{RETRIES}} -gt 0 ]; do
    RETRIES=$(( ${{RETRIES}} - 1 ))
    CHECK_CEPH_HEALTH=$(echo "${{CHECK_CEPH_STATUS}}" | awk '$1 == "health:" {{print $2}}')
    CEPH_STATUS_DATA=$(echo "${{CHECK_CEPH_STATUS}}" | grep 'data:' -A 4)
    NUM_POOL_PGS=$(echo "${{CEPH_STATUS_DATA}}" | awk '$1 == "pools:" && $5 == "pgs" {{print $4}}')
    NUM_ACTIVE_CLEAN_PGS=$(echo "${{CEPH_STATUS_DATA}}" | awk '$1 == "pgs:" && $3 == "active+clean" {{print $2}}')
    if [ ! -z "${{NUM_POOL_PGS}}" ] && [ ! -z "${{NUM_ACTIVE_CLEAN_PGS}}" ] && [ "${{CHECK_CEPH_HEALTH}}" = "HEALTH_OK" ] && [ ${{NUM_POOL_PGS}} -eq ${{NUM_ACTIVE_CLEAN_PGS}} ]; then
      break
    fi
    sleep 5
    echo "[INFO] still waiting..."
  done
  
  # verify osd is no longer in osd tree
  if ceph osd tree | awk -v OSD="osd.${{id}}" '$4 == OSD {{rc = 1; print $4}}; END {{ exit !rc }}'; then
    echo "[WARN] osd.${{id}} still in osd tree"
    exit 1
  fi
done

# remove host from crushmap
# ceph will still throw a exit code of 0 if it doesn't exist
FMT_OSD_HOST=$(echo "${{OSD_HOST}}" | tr '.' '-')
if ceph osd crush rm ${{FMT_OSD_HOST}}; then
  echo "[WARN] failed to remove ${{FMT_OSD_HOST}} from crushmap"
  exit 2
fi
ALT_OSD_HOST=$(echo "${{FMT_OSD_HOST}}" | cut -d'.' -f -1)
if ceph osd crush rm ${{ALT_OSD_HOST}}; then
  echo "[WARN] failed to remove ${{ALT_OSD_HOST}} from crushmap"
  exit 2
fi

# verify host not in crushmap via osd tree
OSD_TREE_HOSTS=$(ceph osd tree | awk '{{print $3 " " $4}}')
if echo "${{OSD_TREE_HOSTS}}" | grep -q "^host ${{OSD_HOST}}$"; then
  echo "[WARN] host ${{OSD_HOST}} is still in crushmap"
  exit 2
fi
if echo "${{OSD_TREE_HOSTS}}" | grep -q "^host ${{ALT_OSD_HOST}}$"; then
  echo "[WARN] host ${{ALT_OSD_HOST}} is still in crushmap"
  exit 2
fi

echo "${{SCRIPT_COMPLETED_STR}}"
"""

# TODO: sometimes rook mon pod still gets assigned to node we just removed even after cordoning and what not
#       and at the moment, requires scaling mon pod to 0 then bouncing the operator pod
#       ...need to find a way to delete rook's cache? and have it assign correctly to new node without bouncing
#       ...possibly remove node from mon endpoints configmap in rook-ceph?

# TODO: sleep between toggle of cephcluster flag may need to be increased...check cephcluster reload interval

# TODO: verify cephcluster toggle is applied
#       (add a get right after, and somehow check it in rook operator logs and debug output)


def delete_rook_ceph_operator_pods(api, rook_ceph_operator_namespace='rook-ceph'):
    logger.info('Deleting rook ceph operator pods in {} namespace'.format(rook_ceph_operator_namespace))
    try:
        api.delete_collection_namespaced_pod(
            namespace=rook_ceph_operator_namespace,
            label_selector="app=rook-ceph-operator",
            watch=False)
    except ApiException as e:
        logger.info('Failed to delete rook ceph operator pods in {} namespace: {}'.format(
            rook_ceph_operator_namespace, e))


def cleanup_rook_ceph_osd_prepare_jobs(batch_api, node_name, rook_ceph_osd_namespace='rook-ceph'):
    logger.info('Cleaning up rook-ceph-osd-prepare jobs associated with node {}'.format(node_name))
    try:
        jobs = batch_api.list_namespaced_job(
            namespace=rook_ceph_osd_namespace,
            label_selector='app=rook-ceph-osd-prepare',
        )
        for job in jobs.items:
            if ('kubernetes.io/hostname' in job.spec.template.spec.node_selector and
                    job.spec.template.spec.node_selector['kubernetes.io/hostname'] == node_name):
                logger.debug('Deleting {} job'.format(job.metadata.name))
                batch_api.delete_namespaced_job(
                    name=job.metadata.name,
                    namespace=rook_ceph_osd_namespace,
                )
                # 'job-name={}'.format(job.metadata.name)
    except ApiException as e:
        logger.exception('Failed to cleanup rook-ceph-osd-prepare jobs associated with node {}: {}'.format(
            node_name, e))


def cleanup_rook_ceph_osd_status_configmaps(api, node_name, rook_ceph_osd_namespace='rook-ceph'):
    logger.info('Cleaning up rook-ceph-osd status configmaps associated with node {}'.format(node_name))
    try:
        api.delete_collection_namespaced_config_map(
            namespace=rook_ceph_osd_namespace,
            label_selector='app=rook-ceph-osd,node={}'.format(node_name),
        )
    except ApiException as e:
        logger.exception('Failed to cleanup rook-ceph-osd status configmaps associated with node {}: {}'.format(
            node_name, e))


def remove_node_from_mon_endpoints_configmap_and_secret(api, node_name, rook_ceph_mon_namespace='rook-ceph'):
    mon_configmap_name = '{}-mon-endpoints'.format(rook_ceph_mon_namespace)
    mon_secret_name = '{}-config'.format(rook_ceph_mon_namespace)
    ip = node_name.partition('.')[0].partition('-')[2].replace('-', '.')

    try:
        logger.debug('Reading {} configmap in {}'.format(mon_configmap_name, rook_ceph_mon_namespace))
        mon_configmap = api.read_namespaced_config_map(mon_configmap_name, rook_ceph_mon_namespace)
        logger.debug('Current {}:\n{}'.format(mon_configmap_name, mon_configmap.data))

        mon_ids = set()

        if 'csi-cluster-config-json' in mon_configmap.data:
            csi = json.loads(mon_configmap.data['csi-cluster-config-json'])
            indices_removal = []
            for i, m in enumerate(csi[0]['monitors']):
                if '{}:'.format(ip) in m:
                    indices_removal.append(i)
            for i in reversed(indices_removal):
                removed = csi[0]['monitors'].pop(i)
                logger.debug('Removed {} from csi-cluster-config-json in {} configmap'.format(
                    removed, mon_configmap_name))
            mon_configmap.data['csi-cluster-config-json'] = "'{}'".format(json.dumps(csi))

        if 'data' in mon_configmap.data:
            data = mon_configmap.data['data'].split(',')
            indices_removal = []
            for i, d in enumerate(data):
                if '={}:'.format(ip) in d:
                    indices_removal.append(i)
            for i in reversed(indices_removal):
                removed = data.pop(i)
                mon_ids.add(removed.partition('=')[0])
                logger.debug('Removed {} from data in {} configmap'.format(removed, mon_configmap_name))
            mon_configmap.data['data'] = ','.join(data)

        if 'mapping' in mon_configmap.data:
            mapping = json.loads(mon_configmap.data['mapping'])
            keys_removal = []
            for k, v in mapping['node'].items():
                if v['Name'] == node_name:
                    keys_removal.append(k)
            for k in keys_removal:
                removed = mapping['node'].pop(k)
                mon_ids.add(k)
                logger.debug('Removed {} from mapping in {} configmap'.format(removed, mon_configmap_name))
            mon_configmap.data['mapping'] = "'{}'".format(json.dumps(mapping))

        logger.debug('Updating {} configmap in {} to:\n{}'.format(
            mon_configmap_name, rook_ceph_mon_namespace, mon_configmap.data))
        api.patch_namespaced_config_map(
            name=mon_configmap_name,
            namespace=rook_ceph_mon_namespace,
            body=mon_configmap)

        logger.debug('Reading {} secret in {}'.format(mon_secret_name, rook_ceph_mon_namespace))
        mon_secret = api.read_namespaced_secret(mon_secret_name, rook_ceph_mon_namespace)
        logger.debug('Current {}:\n{}'.format(mon_secret_name, mon_secret.data))
        indices_removal = []

        if 'mon_initial_members' in mon_secret.data:
            mon_initial_members = base64.b64decode(mon_secret.data['mon_initial_members']).decode()
            mon_initial_members_list = mon_initial_members.split(',')
            for i, mon_id in enumerate(mon_initial_members_list):
                if mon_id in mon_ids:
                    indices_removal.append(i)
            for i in reversed(indices_removal):
                removed = mon_initial_members_list.pop(i)
                logger.debug('Removed {} from mon_initial_members in {} secret'.format(removed, mon_secret_name))
            mon_secret.data['mon_initial_members'] = base64.b64encode(
                ','.join(mon_initial_members_list).encode('UTF-8')).decode('ascii')

        if 'mon_host' in mon_secret.data:
            mon_host = base64.b64decode(mon_secret.data['mon_host']).decode()
            if mon_host.startswith('[') and mon_host.endswith(']'):
                mon_host_list = mon_host[1:-1].split('],[')
            else:
                mon_host_list = mon_host.split(',')
            for i in reversed(indices_removal):
                removed = mon_host_list.pop(i)
                logger.debug('Removed {} from mon_host in {} secret'.format(removed, mon_secret_name))
            if mon_host.startswith('[') and mon_host.endswith(']'):
                mon_secret.data['mon_host'] = base64.b64encode(
                    "[{}]".format('],['.join(mon_host_list)).encode('UTF-8')).decode('ascii')
            else:
                mon_secret.data['mon_host'] = base64.b64encode(','.join(mon_host_list).encode('UTF-8')).decode('ascii')

        logger.debug('Updating {} secret in {} to:\n{}'.format(
            mon_secret_name, rook_ceph_mon_namespace, mon_secret.data))
        api.patch_namespaced_secret(
            name=mon_secret_name,
            namespace=rook_ceph_mon_namespace,
            body=mon_secret)

    except ApiException as e:
        logger.exception('Failed to update {} configmap: {}'.format(mon_configmap_name, e))


def wait_for_rook_ceph_health_ok(api, rook_ceph_operator_namespace='rook-ceph', sleep_seconds=10, retries=15):
    """Periodically checks if Ceph reports back as HEALTH_OK"""
    logger.info('Removing host and OSD from ceph crushmap')
    rook_ceph_operator_pod = get_any_rook_ceph_operator_pod_name(api, rook_ceph_operator_namespace)
    if not rook_ceph_operator_pod:
        return

    for _ in range(retries):
        exec_cmd = [
            'ceph',
            'status',
        ]
        logger.debug('Checking if ceph is reporting HEALTH_OK...')
        try:
            resp = stream(api.connect_get_namespaced_pod_exec,
                          rook_ceph_operator_pod,
                          rook_ceph_operator_namespace,
                          command=exec_cmd,
                          stderr=True,
                          stdin=False,
                          stdout=True,
                          tty=False,
                          _preload_content=False)
            resp.run_forever(5)
            resp.close()
            status = resp.read_all()
            logger.debug("ceph status: \n{}".format(status))
            for line in status.split('\n'):
                if 'health: ' in line:
                    health = line.partition('health: ')[2]
                    logger.debug('Ceph health: {}'.format(health))
                    if health == 'HEALTH_OK':
                        return
        except ApiException as e:
            logger.exception('Failed to get ceph status: {}'.format(e))
        logger.info('Ceph is not reporting HEALTH_OK yet. Checking again in {} seconds...'.format(sleep_seconds))
        time.sleep(sleep_seconds)


def scale_node_rook_ceph_mon_deployment(apps_api, node_name, rook_ceph_mon_namespace='rook-ceph', replicas=0):
    """Scales mon deployment(s) associated with a node"""
    logger.info('Scaling {}\'s Mon deployments associated with node {} to {} replica(s)'.format(
        rook_ceph_mon_namespace, node_name, replicas))
    try:
        logger.info('Locating rook ceph Mon deployment with label app=rook-ceph-mon, '
                    'will filter further by nodeSelector')
        mon_deployments = apps_api.list_namespaced_deployment(
            namespace=rook_ceph_mon_namespace,
            label_selector='app=rook-ceph-mon',
            include_uninitialized=True,
        )
        patch = {
            "spec": {
                "replicas": replicas
            }
        }
        for dep in mon_deployments.items:
            if ('kubernetes.io/hostname' in dep.spec.template.spec.node_selector and
                    dep.spec.template.spec.node_selector['kubernetes.io/hostname'] == node_name):
                logger.debug('Setting rook-ceph-mon-{} deployment to {} replicas'.format(
                    dep.metadata.labels['mon'], replicas))
                # apps_api.patch_namespaced_deployment(
                #     name='rook-ceph-mon-{}'.format(dep.metadata.labels['mon']),
                #     namespace=rook_ceph_mon_namespace,
                #     body=patch,
                # )
                apps_api.patch_namespaced_deployment_scale(
                    name='rook-ceph-mon-{}'.format(dep.metadata.labels['mon']),
                    namespace=rook_ceph_mon_namespace,
                    body=patch,
                )
    except ApiException as err:
        logger.exception('Failed to scale mon pods on node {} to {} replicas: {}'.format(node_name, replicas, err))


def delete_node_rook_ceph_mon_deployment(apps_api, node_name, rook_ceph_mon_namespace='rook-ceph'):
    """Delete mon deployment(s) associated with a node"""
    logger.info('Deleting {}\'s Mon deployments associated with node {}'.format(
        rook_ceph_mon_namespace, node_name))

    try:
        logger.info('Locating rook ceph Mon deployment with label app=rook-ceph-mon, '
                    'will filter further by nodeSelector')
        mon_deployments = apps_api.list_namespaced_deployment(
            namespace=rook_ceph_mon_namespace,
            label_selector='app=rook-ceph-mon',
            include_uninitialized=True,
        )

        for dep in mon_deployments.items:
            if (dep.spec.template.spec.node_selector and
                    'kubernetes.io/hostname' in dep.spec.template.spec.node_selector and
                    dep.spec.template.spec.node_selector['kubernetes.io/hostname'] == node_name):
                logger.debug('Deleting rook-ceph-mon-{} deployment'.format(dep.metadata.labels['mon']))
                try:
                    apps_api.delete_namespaced_deployment(
                        name='rook-ceph-mon-{}'.format(dep.metadata.labels['mon']),
                        namespace=rook_ceph_mon_namespace,
                    )
                except ApiException as err:
                    if err.status != 404:
                        logger.exception('Failed to delete mon deployment(s) on node {}: {}'.format(node_name, err))

    except ApiException as err:
        logger.exception('Failed to list mon deployment(s) on node {}: {}'.format(node_name, err))
        return

    logger.info('Locating rook ceph Mon replicasets with label app=rook-ceph-mon, '
                'will filter further by nodeSelector')
    try:
        mon_replicasets = apps_api.list_namespaced_replica_set(
            namespace=rook_ceph_mon_namespace,
            label_selector='app=rook-ceph-mon',
            include_uninitialized=True,
        )

        for rs in mon_replicasets.items:
            if (rs.spec.template.spec.node_selector and
                    'kubernetes.io/hostname' in rs.spec.template.spec.node_selector and
                    rs.spec.template.spec.node_selector['kubernetes.io/hostname'] == node_name):
                logger.debug('Deleting rook-ceph-mon-{} replicaset'.format(rs.metadata.labels['mon']))
            try:
                apps_api.delete_namespaced_replica_set(
                    name='rook-ceph-mon-{}'.format(rs.metadata.labels['mon']),
                    namespace=rook_ceph_mon_namespace,
                )
            except ApiException as err:
                if err.status != 404:
                    logger.exception('Failed to delete mon deployment(s) on node {}: {}'.format(node_name, err))

    except ApiException as err:
        logger.exception('Failed to list mon replicasets(s) on node {}: {}'.format(node_name, err))
        return


def get_host_associated_osd_ids(apps_api, node_name, rook_ceph_osd_namespace="rook-ceph"):
    """Retrieves the OSD IDs associated with the host, if there is one"""
    osd_ids = []
    logger.info('Retrieving {}\'s OSD IDs associated with node {}'.format(
        rook_ceph_osd_namespace, node_name))
    try:
        logger.info('Locating rook ceph OSD deployment with labels '
                    'of app=rook-ceph-osd,failure-domain={}'.format(node_name))
        osd_deployments = apps_api.list_namespaced_deployment(
            namespace=rook_ceph_osd_namespace,
            label_selector='app=rook-ceph-osd,failure-domain={}'.format(node_name),
            include_uninitialized=True,
        )
        for dep in osd_deployments.items:
            osd_ids.append(dep.metadata.labels['ceph-osd-id'])
        logger.debug('OSD IDs associated with node {}: {}'.format(node_name, ','.join(osd_ids)))
        return osd_ids
    except ApiException as err:
        logger.exception('Failed to retrieve rook ceph OSDs on node {}: {}'.format(node_name, err))


def get_any_rook_ceph_operator_pod_name(api, rook_ceph_operator_namespace="rook-ceph-system"):
    """Gets the rook ceph operator pod name"""
    try:
        pod_list = api.list_namespaced_pod(
            rook_ceph_operator_namespace,
            label_selector="app=rook-ceph-operator",
            limit=1,
            watch=False)
        if not pod_list.items:
            return None
        return pod_list.items[0].metadata.name
    except ApiException:
        return None


def remove_host_and_osd_from_ceph_crushmap(api, node_name, osd_ids=[], rook_ceph_operator_namespace="rook-ceph-system",
                                           retries=3, return_exception=False):
    """Removes the host and osd entries from the ceph crushmap by base64 serializing the script
    defined at 'remove_host_and_osd_script_fmt' and executing it on the rook ceph operaetor pod
    NOTE: there is no retry, it is just a fire and forget operation, we don't even return anything"""
    logger.info('Removing host and OSD from ceph crushmap')
    rook_ceph_operator_pod = get_any_rook_ceph_operator_pod_name(api, rook_ceph_operator_namespace)
    if not rook_ceph_operator_pod:
        return

    osds = set(osd_ids)
    node_removed = False
    alt_node_removed = False
    for _ in range(retries):
        # encoded_script = CRUSHMAP_UPDATE_SCRIPT_STR_FMT.format(
        #     osd_host=node_name,
        #     list_item_delimiter=LIST_ITEM_DELIMITER,
        #     osd_ids_str=LIST_ITEM_DELIMITER.join(osd_ids),
        #     script_completed_str=SCRIPT_COMPLETED_STR).encode()
        # exec_cmd = [
        #     'base64',
        #     '--decode',
        #     '<<<',
        #     base64.b64encode(encoded_script).decode(),
        #     '|',
        #     '/bin/bash',
        # ]
        # stream(api.connect_get_namespaced_pod_exec,
        #               rook_ceph_operator_pod,
        #               rook_ceph_operator_namespace,
        #               command=exec_cmd,
        #               stderr=True,
        #               stdin=False,
        #               stdout=True,
        #               tty=False,
        #               _preload_content=False)

        osds_to_discard = set()
        for osd_id in osds:
            osd_out = False
            osd_purged = False

            exec_cmd = [
                'ceph',
                'osd',
                'out',
                'osd.{}'.format(osd_id),
            ]
            logger.debug('Exec\'ing: {}'.format(' '.join(exec_cmd)))
            try:
                stream(api.connect_get_namespaced_pod_exec,
                       rook_ceph_operator_pod,
                       rook_ceph_operator_namespace,
                       command=exec_cmd,
                       stderr=True,
                       stdin=False,
                       stdout=True,
                       tty=False,
                       _preload_content=False)
                osd_out = True
            except ApiException as e:
                if e.status == 500 and retries > 0:
                    pass
                if return_exception:
                    raise e

            exec_cmd = [
                'ceph',
                'osd',
                'purge',
                str(osd_id),
                '--yes-i-really-mean-it',
            ]
            logger.debug('Exec\'ing: {}'.format(' '.join(exec_cmd)))
            try:
                stream(api.connect_get_namespaced_pod_exec,
                       rook_ceph_operator_pod,
                       rook_ceph_operator_namespace,
                       command=exec_cmd,
                       stderr=True,
                       stdin=False,
                       stdout=True,
                       tty=False,
                       _preload_content=False)
                osd_purged = True
            except ApiException as e:
                if e.status == 500 and retries > 0:
                    pass
                if return_exception:
                    raise e

            if osd_out and osd_purged:
                osds_to_discard.add(osd_id)

        osds.difference_update(osds_to_discard)

        if not node_removed:
            exec_cmd = [
                'ceph',
                'osd',
                'crush',
                'rm',
                node_name.replace('.', '-'),
            ]
            logger.debug('Exec\'ing: {}'.format(' '.join(exec_cmd)))
            try:
                stream(api.connect_get_namespaced_pod_exec,
                       rook_ceph_operator_pod,
                       rook_ceph_operator_namespace,
                       command=exec_cmd,
                       stderr=True,
                       stdin=False,
                       stdout=True,
                       tty=False,
                       _preload_content=False)
                node_removed = True
            except ApiException as e:
                if e.status == 500 and retries > 0:
                    pass
                if return_exception:
                    raise e

        if not alt_node_removed:
            exec_cmd = [
                'ceph',
                'osd',
                'crush',
                'rm',
                node_name.partition('.')[0],
            ]
            logger.debug('Exec\'ing: {}'.format(' '.join(exec_cmd)))
            try:
                stream(api.connect_get_namespaced_pod_exec,
                       rook_ceph_operator_pod,
                       rook_ceph_operator_namespace,
                       command=exec_cmd,
                       stderr=True,
                       stdin=False,
                       stdout=True,
                       tty=False,
                       _preload_content=False)
                alt_node_removed = True
            except ApiException as e:
                if e.status == 500 and retries > 0:
                    pass
                if return_exception:
                    raise e

        if osds or not node_removed or not alt_node_removed:
            logger.debug('Not all execs were completed successfully, retrying in 5 seconds...')
            time.sleep(5)


def delete_node(api, node_name):
    """Deletes a node from the Kubernetes cluster"""
    logger.info('Deleting node {} from the cluster'.format(node_name))
    try:
        api.delete_node(node_name)
        logger.info('Deleted node {}'.format(node_name))
    except ApiException as err:
        logger.exception('Failed to delete node {}: {}'.format(node_name, err))


def bool_str(b):
    return "true" if b else "false"


# def toggle_rook_ceph_crashcollector_disable(custom_obj_api, rook_ceph_osd_namespace='rook-ceph',
#                                             group='ceph.rook.io', version='v1', plural='cephclusters',
#                                             seconds_to_wait_between_toggle=120):
#     try:
#         original = custom_obj_api.get_namespaced_custom_object(
#             group=group,
#             version=version,
#             namespace=rook_ceph_osd_namespace,
#             plural=plural,
#             name=rook_ceph_osd_namespace
#         )
#
#         original_flag = False  # if not set, use False as default
#
#         if 'crashCollector' in original['spec'] and 'disable' in original['spec']['crashCollector']:
#             original_flag = original['spec']['crashCollector']['disable']
#
#         patch = {
#             "spec": {
#                 "crashCollector": {
#                     "disable": (not original_flag)
#                 }
#             }
#         }
#         logger.debug('Applying patch to cephcluster {}: {}'.format(rook_ceph_osd_namespace, patch))
#         custom_obj_api.patch_namespaced_custom_object(
#             group=group,
#             version=version,
#             namespace=rook_ceph_osd_namespace,
#             plural=plural,
#             name=rook_ceph_osd_namespace,
#             body=patch,
#         )
#         time.sleep(seconds_to_wait_between_toggle)
#
#         patch = {
#             "spec": {
#                 "crashCollector": {
#                     "disable": (not original_flag)
#                 }
#             }
#         }
#         logger.debug('Applying patch to cephcluster {}: {}'.format(rook_ceph_osd_namespace, patch))
#         custom_obj_api.patch_namespaced_custom_object(
#             group=group,
#             version=version,
#             namespace=rook_ceph_osd_namespace,
#             plural=plural,
#             name=rook_ceph_osd_namespace,
#             body=patch,
#         )
#     except ApiException as err:
#         logger.exception('Failed to toggle rook ceph crashcollector disable: {}'.format(err))


# TODO: change sleep time back to 10 seconds after testing
def toggle_rook_ceph_version_allow_unsupported_flag(custom_obj_api, rook_ceph_osd_namespace='rook-ceph',
                                                    group='ceph.rook.io', version='v1', plural='cephclusters',
                                                    seconds_to_wait_between_toggle=10):
    try:
        original = custom_obj_api.get_namespaced_custom_object(
            group=group,
            version=version,
            namespace=rook_ceph_osd_namespace,
            plural=plural,
            name=rook_ceph_osd_namespace
        )

        original_flag = False  # if not set, use False as default
        if 'allowUnsupported' in original['spec']['cephVersion']:
            original_flag = original['spec']['cephVersion']['allowUnsupported']

        patch = {
            "spec": {
                "cephVersion": {
                    "allowUnsupported": (not original_flag)
                }
            }
        }
        logger.debug('Applying patch to cephcluster {}: {}'.format(rook_ceph_osd_namespace, patch))
        custom_obj_api.patch_namespaced_custom_object(
            group=group,
            version=version,
            namespace=rook_ceph_osd_namespace,
            plural=plural,
            name=rook_ceph_osd_namespace,
            body=patch,
        )
        time.sleep(seconds_to_wait_between_toggle)

        patch = {
            "spec": {
                "cephVersion": {
                    "allowUnsupported": original_flag
                }
            }
        }
        logger.debug('Applying patch to cephcluster {}: {}'.format(rook_ceph_osd_namespace, patch))
        custom_obj_api.patch_namespaced_custom_object(
            group=group,
            version=version,
            namespace=rook_ceph_osd_namespace,
            plural=plural,
            name=rook_ceph_osd_namespace,
            body=patch,
        )
    except ApiException as err:
        logger.exception('Failed to toggle rook ceph version allow unsupported flag: {}'.format(err))


def delete_rook_ceph_osd_deployment(apps_api, osd_ids=[], rook_ceph_osd_namespace='rook-ceph'):
    """Delete rook ceph osd deployment by OSD IDs"""
    logger.info('Deleting rook ceph osd {} deployment(s)'.format(', '.join(osd_ids)))
    try:
        apps_api.delete_collection_namespaced_deployment(
            namespace=rook_ceph_osd_namespace,
            label_selector='ceph-osd-id in ({})'.format(', '.join(osd_ids)),
            include_uninitialized=True,
        )
        apps_api.delete_collection_namespaced_replica_set(
            namespace=rook_ceph_osd_namespace,
            label_selector='ceph-osd-id in ({})'.format(', '.join(osd_ids)),
            include_uninitialized=True,
        )

        # osd_deployments = apps_api.list_namespaced_deployment(
        #     namespace=rook_ceph_osd_namespace,
        #     label_selector='ceph-osd-id in ({})'.format(', '.join(osd_ids)),
        #     include_uninitialized=True,
        # )
        # for dep in osd_deployments.items:
        #     logger.info('Deleting deployment {}/{}'.format(rook_ceph_osd_namespace, dep.metadata.name))
        #     apps_api.delete_namespaced_deployment(
        #         name=dep.metadata.name,
        #         namespace=rook_ceph_osd_namespace,
        #     )
        #     logger.info('Deleted deployment {}/{}'.format(rook_ceph_osd_namespace, dep.metadata.name))
        #
        # logger.debug('Locating rook ceph osd replicaset corresponding to osd(s) {}'.format(', '.join(osd_ids)))
        # osd_replicasets = apps_api.list_namespaced_replica_set(
        #     namespace=rook_ceph_osd_namespace,
        #     label_selector='ceph-osd-id in ({})'.format(', '.join(osd_ids)),
        #     include_uninitialized=True,
        # )
        # for rs in osd_replicasets.items:
        #     logger.info('Deleting replicaset {}/{}'.format(rook_ceph_osd_namespace, rs.metadata.name))
        #     apps_api.delete_namespaced_replica_set(
        #         name=rs.metadata.name,
        #         namespace=rook_ceph_osd_namespace,
        #     )
        #     logger.info('Deleted replicaset {}/{}'.format(rook_ceph_osd_namespace, rs.metadata.name))
    except ApiException as err:
        if err.status != 404:
            logger.exception('Failed to delete rook ceph osd(s) {}: {}'.format(', '.join(osd_ids), err))


def delete_rook_ceph_crashcollector(apps_api, rook_ceph_crashcollectors_namespace, node_name):
    """Delete rook ceph crashcollector deployment and underlying replicaset corresponding to node"""
    logger.info('Deleting {}\'s rook ceph crashcollector on node {}'.format(
        rook_ceph_crashcollectors_namespace, node_name))
    try:
        apps_api.delete_collection_namespaced_deployment(
            namespace=rook_ceph_crashcollectors_namespace,
            label_selector='app=rook-ceph-crashcollector,node_name={}'.format(node_name),
            include_uninitialized=True,
        )
        apps_api.delete_collection_namespaced_replica_set(
            namespace=rook_ceph_crashcollectors_namespace,
            label_selector='app=rook-ceph-crashcollector,node_name={}'.format(node_name),
            include_uninitialized=True,
        )

        # logger.info('Locating rook ceph crashcollector deployment corresponding to node {}'.format(node_name))
        # crashcollector_deployments = apps_api.list_namespaced_deployment(
        #     namespace=rook_ceph_crashcollectors_namespace,
        #     label_selector='app=rook-ceph-crashcollector,node_name={}'.format(node_name),
        #     include_uninitialized=True,
        # )
        # for dep in crashcollector_deployments.items:
        #     logger.info('Deleting deployment {}/{}'.format(rook_ceph_crashcollectors_namespace, dep.metadata.name))
        #     apps_api.delete_namespaced_deployment(
        #         name=dep.metadata.name,
        #         namespace=rook_ceph_crashcollectors_namespace,
        #     )
        #     logger.info('Deleted deployment {}/{}'.format(rook_ceph_crashcollectors_namespace, dep.metadata.name))
        #
        # logger.debug('Locating rook ceph crashcollector replicaset corresponding to node {}'.format(node_name))
        # crashcollector_replicasets = apps_api.list_namespaced_replica_set(
        #     namespace=rook_ceph_crashcollectors_namespace,
        #     label_selector='app=rook-ceph-crashcollector,node_name={}'.format(node_name),
        #     include_uninitialized=True,
        # )
        # for rs in crashcollector_replicasets.items:
        #     logger.info('Deleting replicaset {}/{}'.format(rook_ceph_crashcollectors_namespace, rs.metadata.name))
        #     apps_api.delete_namespaced_replica_set(
        #         name=rs.metadata.name,
        #         namespace=rook_ceph_crashcollectors_namespace,
        #     )
        #     logger.info('Deleted replicaset {}/{}'.format(rook_ceph_crashcollectors_namespace, rs.metadata.name))
    except ApiException as err:
        logger.exception('Failed to delete rook ceph crashcollector on node {}: {}'.format(node_name, err))


def detach_node_rook_volumes(custom_obj_api, rook_ceph_volumes_namespace, node_name):
    """Detaches the rook volumes (apiVersion: rook.io/v1alpha2, Kind: Volume) that are attached to the node"""
    logger.info('Getting list of rook volumes')
    rook_volumes = custom_obj_api.list_namespaced_custom_object(
        group='rook.io',
        version='v1alpha2',
        namespace=rook_ceph_volumes_namespace,
        plural='volumes',
        watch=False,
    )

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
    logger.info('Cordoning node {}'.format(node_name))
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


def get_instance_node_mapping(api):
    nodes = api.list_node(include_uninitialized=True, pretty=True).items
    mapping = dict()
    for n in nodes:
        instance_id = n.spec.provider_id.split('/')[-1]
        mapping[instance_id] = n.metadata.name
    return mapping


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
