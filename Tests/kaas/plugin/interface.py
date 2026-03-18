from abc import ABC, abstractmethod


class KubernetesClusterPlugin(ABC):
    """
    Abstract base class for Kubernetes cluster plugins.
    Defines the interface that all cluster plugins must implement.
    """

    @abstractmethod
    def __init__(self, plugin_config, basepath='.', cwd='.', name=None):
        """
        Initializes the plugin with its configuration.
        :param plugin_config: A dictionary containing the plugin-specific configuration.
        :param basepath: The base directory for template files.
        :param cwd: The current working directory for writing output files.
        :param name: The name of the test run / cluster.
        """
        raise NotImplementedError

    @abstractmethod
    def create_cluster(self):
        """Creates and provisions a Kubernetes cluster."""
        raise NotImplementedError

    @abstractmethod
    def delete_cluster(self):
        """Deletes/tears down the Kubernetes cluster."""
        raise NotImplementedError
