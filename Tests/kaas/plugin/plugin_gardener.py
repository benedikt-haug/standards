import logging
import os
import os.path
import time

from jinja2 import Environment, StrictUndefined
from kubernetes.client import ApiClient, ApiException
import yaml

from .interface import KubernetesClusterPlugin
from . import gardener_helper as _gh

logger = logging.getLogger(__name__)
logging.getLogger("kubernetes").setLevel(logging.INFO)


TEMPLATE_KEYS = ('shoot', 'kubeconfig')


class _ShootOps:
    def __init__(self, namespace: str, name: str):
        self.namespace = namespace
        self.name = name
        self.secret_name = f'{name}.kubeconfig'

    def _get_last_operation(self, co_api: _gh.CustomObjectsApi):
        # be defensive here, for none of the fields need exist in early stages of the object's life
        try:
            shoot = co_api.get_namespaced_custom_object(group=_gh.GARDENER_GROUP, version=_gh.GARDENER_VERSION, namespace=self.namespace, plural=_gh.GARDENER_PLURAL, name=self.name)
            return shoot.get('status', {}).get('lastOperation')
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def _is_shoot_ready(self, co_api: _gh.CustomObjectsApi):
        try:
            shoot = co_api.get_namespaced_custom_object(group=_gh.GARDENER_GROUP, version=_gh.GARDENER_VERSION, namespace=self.namespace, plural=_gh.GARDENER_PLURAL, name=self.name)
        except ApiException as e:
            if e.status == 404:
                return False # Shoot not found, so not ready
            raise

        last_op = shoot.get('status', {}).get('lastOperation')
        if not last_op or last_op.get('state') != 'Succeeded':
            return False

        return True # Creation is Succeeded

    def create(self, *, co_api: _gh.CustomObjectsApi, shoot_dict):
        # Gardener shoot reconciliation is idempotent, so we can just try to create.
        # If it exists, we check its state.
        while True:
            logger.debug(f'creating shoot object for {self.name}')
            try:
                _gh.create_shoot(co_api=co_api, namespace=self.namespace, body=shoot_dict)
                break
            except ApiException as e:
                # 409 means that the object already exists
                if e.status != 409:
                    raise

                last_op = self._get_last_operation(co_api)
                if last_op is None:
                    logger.debug(f'shoot object for {self.name} was present, has disappeared, retry')
                    continue

                state = last_op.get('state', 'Unknown')
                logger.debug(f'shoot object for {self.name} already present in state {state}')

                if state in ('Succeeded', 'Processing'):
                    break
                elif state == 'Failed':
                    raise RuntimeError(f"Shoot {self.name} is in Failed state. Please check the Gardener dashboard.")

                logger.debug(f'waiting 30 s for shoot {self.name} to proceed')
                time.sleep(30)

    def delete(self, co_api: _gh.CustomObjectsApi):
        # Add deletion confirmation annotation to the shoot before deleting.
        # This is required by Gardener.
        shoot_patch = {
            "metadata": {
                "annotations": {
                    "confirmation.gardener.cloud/deletion": "true"
                }
            }
        }
        co_api.patch_namespaced_custom_object(
            group=_gh.GARDENER_GROUP, version=_gh.GARDENER_VERSION, namespace=self.namespace,
            plural=_gh.GARDENER_PLURAL, name=self.name, body=shoot_patch)
        try:
            _gh.delete_shoot(co_api=co_api, namespace=self.namespace, name=self.name)
        except ApiException as e:
            if e.status == 404:
                logger.debug(f'shoot {self.name} not present')
                return
            raise

        logger.debug(f'shoot {self.name} deletion requested; waiting 8 s for it to start deleting')
        time.sleep(8)
        while True:
            last_op = self._get_last_operation(co_api)
            if last_op is None:
                logger.info(f"Shoot {self.name} has been deleted.")
                break

            state = last_op.get('state', 'Unknown')
            op_type = last_op.get('type', 'Unknown')

            if op_type == 'Delete' and state == 'Processing':
                progress = last_op.get('progress', 0)
                logger.debug(f'shoot {self.name} is being deleted (progress: {progress}%); waiting 30s')
                time.sleep(30)
            elif op_type == 'Delete' and state == 'Succeeded':
                 logger.info(f"Shoot {self.name} deletion succeeded, but object still exists. Waiting for it to vanish.")
                 time.sleep(30)
            else:
                raise RuntimeError(f'shoot {self.name} in unexpected state during deletion: type={op_type} state={state}')

    def wait_for_shoot_ready(self, co_api: _gh.CustomObjectsApi):
        while not self._is_shoot_ready(co_api):
            last_op = self._get_last_operation(co_api)
            state = "N/A"
            progress = "N/A"
            if last_op:
                state = last_op.get('state', 'Unknown')
                progress = last_op.get('progress', 0)

            logger.debug(f'waiting 30 s for shoot {self.name} to become ready (current state: {state}, progress: {progress}%)')
            time.sleep(30)
        logger.debug(f'shoot {self.name} appears to be ready')


def load_templates(env, basepath, fn_map, keys=TEMPLATE_KEYS):
    new_map = {}
    for key in keys:
        fn = fn_map.get(key)
        if fn is None:
            new_map[key] = None
            continue
        with open(os.path.join(basepath, fn), "r") as fileobj:
            new_map[key] = env.from_string(fileobj.read())
    missing = [key for k, v in new_map.items() if v is None]
    if missing:
        raise RuntimeError(f'missing templates: {", ".join(missing)}')
    return new_map


class PluginGardener(KubernetesClusterPlugin):
    """
    Plugin to handle the provisioning of Kubernetes clusters via Gardener
    to be used for conformance testing.
    """
    def __init__(self, plugin_config, basepath='.', cwd='.', name=None):
        self.basepath = basepath
        self.cwd = cwd
        self.config = plugin_config
        self.env = Environment(undefined=StrictUndefined)

        # Render secrets from environment variables
        secrets = self.config.get('secrets', {}).copy()
        for key, value in secrets.items():
            if isinstance(value, str) and '{{' in value:
                template = self.env.from_string(value)
                secrets[key] = template.render(env=os.environ)

        self.template_map = load_templates(self.env, self.basepath, self.config['templates'])
        # Combine all variables for template rendering
        self.template_vars = {**self.config.get('vars', {}), **secrets, 'name': name}
        self.name = name
        self.kubeconfig = yaml.load(self._render_template('kubeconfig'), Loader=yaml.SafeLoader)
        self.client_config = _gh.Configuration()
        _gh.setup_client_config(self.client_config, self.kubeconfig, cwd=self.cwd)
        self.namespace = self.kubeconfig['contexts'][0]['context']['namespace']

    def _render_template(self, key):
        return self.template_map[key].render(**self.template_vars)

    def _write_shoot_yaml(self, shoot_yaml):
        # write out shoot.yaml for purposes of documentation
        shoot_yaml_path = os.path.join(self.cwd, 'shoot.yaml')
        logger.debug(f'writing out {shoot_yaml_path}')
        with open(shoot_yaml_path, "w") as fileobj:
            fileobj.write(shoot_yaml)

    def _write_kubeconfig(self, kubeconfig):
        # write out kubeconfig.yaml
        kubeconfig_path = os.path.join(self.cwd, 'kubeconfig.yaml')
        logger.debug(f'writing out {kubeconfig_path}')
        with open(kubeconfig_path, 'wb') as fileobj:
            fileobj.write(kubeconfig)

    def create_cluster(self):
        logger.info("--- Garden Kubeconfig being used ---")
        logger.info(f"\n{yaml.dump(self.kubeconfig)}")
        logger.info("------------------------------------")

        with ApiClient(self.client_config) as api_client:
            core_api = _gh.CoreV1Api(api_client)
            co_api = _gh.CustomObjectsApi(api_client)

            shoot_yaml = self._render_template('shoot')

            logger.info("--- Shoot Manifest to be applied ---")
            logger.info(f"\n{shoot_yaml}")
            logger.info("------------------------------------")

            shoot_dict = yaml.load(shoot_yaml, Loader=yaml.SafeLoader)
            if not shoot_dict:
                raise ValueError("Failed to load shoot.yaml: template rendering resulted in an empty document. "
                                 "Please check that all required variables are defined in config.yaml and have values.")
            self._write_shoot_yaml(shoot_yaml)
            sops = _ShootOps(self.namespace, self.name) # Initialize sops here
            sops.create(co_api=co_api, shoot_dict=shoot_dict) # Create the shoot
            sops.wait_for_shoot_ready(co_api) # Wait for the shoot to reach Succeeded state

    def delete_cluster(self):
        with ApiClient(self.client_config) as api_client:
            co_api = _gh.CustomObjectsApi(api_client)
            _ShootOps(self.namespace, self.name).delete(co_api)
