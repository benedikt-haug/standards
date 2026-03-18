import base64

from kubernetes.client import Configuration, CoreV1Api, CustomObjectsApi


GARDENER_GROUP = 'core.gardener.cloud'
GARDENER_VERSION = 'v1beta1'
GARDENER_PLURAL = 'shoots'


def setup_client_config(client_config: Configuration, kubeconfig: dict, cwd='.'):
    """
    Set up the Kubernetes client configuration from a kubeconfig dict.
    This is a simplified version of what `kubernetes.config.load_kube_config` does.
    """
    context = kubeconfig['contexts'][0]
    cluster_name = context['context']['cluster']
    user_name = context['context']['user']

    cluster = next(c['cluster'] for c in kubeconfig['clusters'] if c['name'] == cluster_name)
    user = next(u['user'] for u in kubeconfig['users'] if u['name'] == user_name)

    client_config.host = cluster['server']

    ca_data = cluster.get('certificate-authority-data')
    if ca_data:
        client_config.ssl_ca_cert = f"{cwd}/ca.crt"
        with open(client_config.ssl_ca_cert, "wb") as f:
            f.write(base64.b64decode(ca_data))

    client_cert_data = user.get('client-certificate-data')
    client_key_data = user.get('client-key-data')
    if client_cert_data and client_key_data:
        client_config.cert_file = f"{cwd}/client.crt"
        client_config.key_file = f"{cwd}/client.key"
        with open(client_config.cert_file, "wb") as f:
            f.write(base64.b64decode(client_cert_data))
        with open(client_config.key_file, "wb") as f:
            f.write(base64.b64decode(client_key_data))

    token = user.get('token')
    if token:
        client_config.api_key_prefix['authorization'] = 'Bearer'
        client_config.api_key['authorization'] = token


def create_shoot(*, co_api: CustomObjectsApi, namespace: str, body: dict):
    return co_api.create_namespaced_custom_object(
        group=GARDENER_GROUP,
        version=GARDENER_VERSION,
        namespace=namespace,
        plural=GARDENER_PLURAL,
        body=body,
    )


def delete_shoot(*, co_api: CustomObjectsApi, namespace: str, name: str):
    return co_api.delete_namespaced_custom_object(
        group=GARDENER_GROUP, version=GARDENER_VERSION, namespace=namespace, plural=GARDENER_PLURAL, name=name
    )


def get_secret_data(core_api: CoreV1Api, namespace: str, name: str):
    secret = core_api.read_namespaced_secret(name, namespace)
    return base64.b64decode(secret.data['kubeconfig'])
