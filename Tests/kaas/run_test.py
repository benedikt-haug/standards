import argparse
import importlib
import logging
import os
import sys

from jinja2 import Environment
import yaml


def _get_plugin_class(plugin_name):
    """Dynamically import and return the plugin class."""
    module_name = f"plugin.plugin_{plugin_name}"
    class_name = f"Plugin{plugin_name.capitalize()}"
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        logging.error(f"Could not load plugin '{plugin_name}': {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Run KaaS cluster tests.")
    parser.add_argument("--config", required=True, help="Path to the test configuration file.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--create", action="store_true", help="Create the cluster.")
    group.add_argument("--delete", action="store_true", help="Delete the cluster.")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    config_path = args.config
    base_path = os.path.dirname(config_path)
    cwd = os.path.join(base_path, 'run')
    os.makedirs(cwd, exist_ok=True)

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    plugin_name = config['plugin']['name']
    PluginClass = _get_plugin_class(plugin_name)

    logging.info(f"Initializing plugin: {plugin_name}")
    plugin_instance = PluginClass(config['plugin'], basepath=base_path, cwd=cwd, name=config['name'])

    try:
        if args.create:
            logging.info(f"Creating cluster '{config['plugin']['name']}'...")
            plugin_instance.create_cluster()
            logging.info("Cluster creation process finished successfully.")
        elif args.delete:
            logging.info(f"Deleting cluster '{config['plugin']['name']}'...")
            plugin_instance.delete_cluster()
            logging.info("Cluster deletion process finished successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=args.debug)
        sys.exit(1)


if __name__ == "__main__":
    main()
