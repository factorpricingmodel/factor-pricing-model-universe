import importlib
from typing import Any, Callable, Dict, List, Optional

import yaml


class DelayedDataObject:
    """
    Delayed data object.

    The object is a placeholder in the JSON configuration to
    load the data from either external source or data pipeline.
    """

    _DATA_MAPPING = {}

    def __init__(self, name):
        """
        Constructor.

        Parameters
        ----------
        name : str
            Name of the data object.
        """
        self._name = name

    def __eq__(self, other) -> bool:
        """
        Equal operator.

        The object is regarded as the same if its name is the same.

        Parameters
        ----------
        other : DelayedDataObject
            Delayed data object to compare.
        """
        if not isinstance(other, DelayedDataObject):
            return False

        return self._name == other._name

    @property
    def values(self):
        """
        Get the values of the object.

        :return: The values of the object.
        """
        return DelayedDataObject._DATA_MAPPING[self._name]

    @classmethod
    def update_values(cls, name: str, values: Any) -> None:
        """
        Update the values in the cache.

        Parameters
        ----------
        name : str
            Name of the data object.
        values: Any
            Values of the data object.
        """
        cls._DATA_MAPPING[name] = values

    @classmethod
    def get_values(cls, name: str) -> Any:
        """
        Get the values of the object from the cache.

        Parameters
        ----------
        name : str
            Name of the data object.
        """
        return cls._DATA_MAPPING[name]


def data_representer(dumper, data):
    # fmt: off
    return dumper.represent_scalar("!data", "%s" % data)
    # fmt: on


def data_constructor(loader, node):
    name = loader.construct_scalar(node)
    return DelayedDataObject(name=name)


# fmt: off
yaml.add_constructor("!data", data_constructor)
# fmt: on


class Configuration:
    """
    Configuration.

    The class parse the configuration in a file handler or a
    text to specified data attributes, e.g. the output directory.
    """

    def __init__(self, stream, parameters: Optional[Dict[str, str]] = None):
        """
        Parameters:
        -----------
        stream: File stream.
            The file stream to read configuration from.
        parameters: Optional dictionary.
            The parameters to format in the parsed configuration.
        """
        config = yaml.load(stream, Loader=yaml.Loader)
        config = Configuration._resolve_parameters(config, parameters)
        self._config = config
        self.output_filename = Configuration._get_config(
            self._config, "output_filename"
        )
        self.intermediate_directory = Configuration._get_config(
            self._config, "intermediate_directory"
        )
        self.pipelines = Configuration._get_config(self._config, "pipeline")
        self.datas = Configuration._get_config(self._config, "data")

    @classmethod
    def _resolve_parameters(
        cls, items: Any, parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Resolves the parameters.

        Parameters
        ----------
        items : Any
            Any types of items to resolve parameters.
        parameters : dict
            Parameters.

        Returns
        -------
        The JSON like dictionary.
        """
        if not parameters:
            return items

        if isinstance(items, str):
            return items.format(**parameters)
        elif isinstance(items, list):
            return [
                Configuration._resolve_parameters(item, parameters) for item in items
            ]
        elif isinstance(items, dict):
            return {
                Configuration._resolve_parameters(key, parameters): (
                    Configuration._resolve_parameters(item, parameters)
                )
                for key, item in items.items()
            }

        return items

    @classmethod
    def _get_config(cls, config: Dict[str, Any], key: str) -> Any:
        """
        Get a configuration with key.

        Parameters
        ----------
        config : dict
            Json like configuration object
        key: string
            Key name to look up
        """
        try:
            return config[key]
        except KeyError:
            raise KeyError(
                f"Key {key} is not found in configuration object."
                f"Available keys are: {list(config.keys())}"
            )


class DataStore:
    """
    DataStore.
    """

    def __init__(
        self,
        config: Configuration,
        custom_functions: Optional[Dict[str, Callable]] = None,
    ):
        """
        Parameters:
        -----------
        config: Configuration
            Configuration object.
        """
        self._config_datas = config.datas
        self._data_store = {
            name: DelayedDataObject(name=name) for name in config.datas.keys()
        }
        self._custom_functions = custom_functions

    def get(self, name: str):
        """
        Get a data object.

        Parameters
        ----------
        name: string
            Name of the data object.
        """
        try:
            data_object = self._data_store[name]
        except KeyError:
            raise KeyError(f"Key {name} is not found in the data store")

        try:
            return data_object.values
        except KeyError:
            assert (
                name in self._config_datas
            ), f"Key {name} should be found in configuration"
            data_config = self._config_datas[name]
            function_name = data_config["function"]
            parameters = {}
            for param_name, parameter in data_config.get("parameters", {}).items():
                if not isinstance(parameter, DelayedDataObject):
                    parameters[param_name] = parameter

                try:
                    parameters[param_name] = parameter.values
                except KeyError:
                    parameter_values = self.get(name=param_name)
                    DelayedDataObject.update_values(param_name, parameter_values)
                    parameters[param_name] = parameter_values

            values = self._run_function(
                function_name=function_name, parameters=parameters
            )
            DelayedDataObject.update_values(name, values)
            return values

    def _run_function(self, function_name: str, parameters: Dict[str, Any]) -> Any:
        """
        Run a function.

        Parameters
        ----------
        function_name: string
            Name of the function to run.
        parameters: Dict[str, Any]
            Parameters to run the function with.
        """
        data_module = importlib.import_module("factor_pricing_model_universe.data")
        try:
            function = getattr(data_module, function_name)
        except AttributeError:
            try:
                function = self._custom_functions[function_name]
            except KeyError:
                raise ValueError(
                    f"Callable name {function_name} cannot be found "
                    "neither in the data module nor the customized functions"
                )

        if function is None:
            raise RuntimeError(
                "Failed to catch the exception to locate the "
                f"function {function_name}"
            )

        return function(**parameters)
