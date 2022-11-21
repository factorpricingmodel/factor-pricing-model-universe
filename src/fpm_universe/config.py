import importlib
from typing import Any, Callable, Dict, Optional

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
    def name(self) -> str:
        """
        Return the name of the data object.
        """
        return self._name

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
        self.start_datetime = Configuration._get_config(self._config, "start_datetime")
        self.last_datetime = Configuration._get_config(self._config, "last_datetime")
        self.frequency = Configuration._get_config(self._config, "frequency")
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
            try:
                return items.format(**parameters)
            except KeyError:
                raise KeyError(
                    f"Failed to format items {items} with parameters {parameters}"
                )
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
            name: DelayedDataObject(name=f"{id(self)}-{name}")
            for name in config.datas.keys()
        }
        self._custom_functions = custom_functions

    def update_values(self, name: str, values: Any):
        """
        Update the key values.

        Parameters
        ----------
        name: string
            Name of the data object.
        values: Any
            Values of the data object.
        """
        name = f"{id(self)}-{name}"
        DelayedDataObject.update_values(name, values)

    def get(self, name: str) -> Any:
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
            try:
                data_config = self._config_datas[name]
            except KeyError:
                raise KeyError(f"Key {name} is not found in configuration")
            function_name = data_config["function"]
            parameters = {}
            for param_name, parameter in data_config.get("parameters", {}).items():
                if isinstance(parameter, DelayedDataObject):
                    try:
                        parameter_values = parameter.values
                    except KeyError:
                        parameter_values = self.get(name=parameter.name)
                        self.update_values(parameter.name, parameter_values)
                elif isinstance(parameter, dict):
                    parameter_values = {
                        pm: self.get(name=pv.name)
                        if isinstance(pv, DelayedDataObject)
                        else pv
                        for pm, pv in parameter.items()
                    }
                elif isinstance(parameter, list):
                    parameter_values = [
                        self.get(name=pv.name)
                        if isinstance(pv, DelayedDataObject)
                        else pv
                        for pv in parameter
                    ]
                else:
                    parameter_values = parameter

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
        data_module = importlib.import_module("fpm_universe.data")
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


class PipelineExecutor:
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
        custom_functions: Optional[Dict[str, Callable]]
            Custom functions of pipelines.
        """
        self._config = config
        self._custom_functions = custom_functions

    @staticmethod
    def execute(
        data_store: DataStore,
        config: Configuration,
        pipeline: Dict[str, Any],
        custom_functions: Optional[Dict[str, Callable]] = None,
    ) -> Any:
        """
        Execute a single pipeline.

        Parameters
        ----------
        data_store: DataStore
            Data store to execute the pipeline with.
        config: Configuration
            Configuration object.
        pipeline: Dict[str, Any]
            Pipeline configuration.
        custom_functions: Optional[Dict[str, Callable]]
            Custom functions of pipelines.
        """
        name = pipeline["name"]
        function_name = pipeline["function"]
        parameters = pipeline.get("parameters", {})
        for param_name, param in parameters.copy().items():
            if not isinstance(param, DelayedDataObject):
                continue
            parameters[param_name] = data_store.get(param.name)
        data_module = importlib.import_module("fpm_universe.pipeline")
        try:
            function = getattr(data_module, function_name)
        except AttributeError:
            try:
                function = custom_functions[function_name]
            except KeyError:
                raise ValueError(
                    f"Callable name {function_name} cannot be found "
                    "neither in the pipeline module nor the customized functions"
                )

        if function is None:
            raise RuntimeError(
                "Failed to catch the exception to locate the "
                f"function {function_name}"
            )

        return name, function(
            start_datetime=config.start_datetime,
            last_datetime=config.last_datetime,
            frequency=config.frequency,
            **parameters,
        )

    def execute_all(self, data_store: DataStore) -> Dict[str, Any]:
        """
        Execute all pipelines.

        Parameters
        ----------
        data_store: DataStore
            Data store to execute the pipeline with.

        Returns
        -------
        Dict[str, Any]
            Dictionary of pipeline returns. Keys are the pipeline names
            and the values are the return values.
        """
        for pipeline in self._config.pipelines:
            yield PipelineExecutor.execute(
                config=self._config,
                pipeline=pipeline,
                data_store=data_store,
                custom_functions=self._custom_functions,
            )
