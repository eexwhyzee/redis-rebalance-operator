import os
import pprint
import time

import kopf
import kubernetes
import yaml
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream


@kopf.on.field('apps', 'v1', 'statefulsets', labels={'app': os.environ['APP_NAME']}, field='spec.replicas')
def update_replica(logger, body, meta, spec, status, old, new, **kwargs):
    logger.info(f'Handling the Replica Update = {old} -> {new}')
    namespace = meta.namespace

    if new > old:
        api = kubernetes.client.CoreV1Api()
        while True:
            try:
                resp = api.read_namespaced_pod(
                    name=f"{os.environ['APP_NAME']}-{new-1}",
                    namespace=namespace
                )
                new_node_ip = resp.status.pod_ip
                logger.info(f'New node IP: {new_node_ip}')
            except ApiException as e:
                logger.info('Waiting for pod to start up...')
                time.sleep(1)
                continue

            if resp.status.phase != 'Pending':
                break
            time.sleep(1)

        # Get pod IP from node in existing cluster
        resp = api.read_namespaced_pod(
                    name=f"{os.environ['APP_NAME']}-0",
                    namespace=namespace
                )
        existing_node_ip = resp.status.pod_ip
        logger.info(f'Existing node ip: {existing_node_ip}')

        # Add node to cluster
        logger.info('Adding node to cluster...')
        exec_command = [
            'redis-cli',
            '--cluster',
            'add-node',
            f"{new_node_ip}:{os.environ['REDIS_PORT']}",
            f"{existing_node_ip}:{os.environ['REDIS_PORT']}",
            ]
        resp = stream(api.connect_get_namespaced_pod_exec,
                f"{os.environ['APP_NAME']}-{new-1}",
                namespace,
                command=exec_command,
                stderr=True, stdin=False,
                stdout=True, tty=False)
        logger.info("Response: " + resp)

        # Prep node for rebalance
        logger.info('Restarting node before rebalance...')
        exec_command = [
            '/sbin/killall5',
            ]
        resp = stream(api.connect_get_namespaced_pod_exec,
                f"{os.environ['APP_NAME']}-{new-1}",
                namespace,
                command=exec_command,
                stderr=True, stdin=False,
                stdout=True, tty=False)

        # Rebalance slots to new empty node
        logger.info('Performing cluster rebalance...')
        exec_command = [
            'redis-cli',
            '--cluster',
            'rebalance',
            f"{existing_node_ip}:{os.environ['REDIS_PORT']}",
            '--cluster-use-empty-masters',
            ]
        resp = stream(api.connect_get_namespaced_pod_exec,
                    f"{os.environ['APP_NAME']}-0",
                    namespace,
                    command=exec_command,
                    stderr=True, stdin=False,
                    stdout=True, tty=False)
        logger.info("Response: " + resp)

    else:
        # Delete PVC
        logger.info('Deleting PVC...')
        api = kubernetes.client.CoreV1Api()
        resp = api.delete_namespaced_persistent_volume_claim(
                    name=f"redis-data-{os.environ['APP_NAME']}-{old-1}",
                    namespace=namespace,
                    body=kubernetes.client.V1DeleteOptions()
                )
