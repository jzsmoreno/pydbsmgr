from configparser import ConfigParser


def load_config(config_file):
    """Load the configuration file.

    Parameters
    ----------
    config_file : `str`
        The path to the configuration file.

    Returns
    -------
    config : `ConfigParser`
        A configuration object loaded from file.
    """

    config = ConfigParser()
    config.read(config_file)

    return config


def parse_config(config):
    """Parse the configuration file.

    Parameters
    ----------
    config : `ConfigParser`
        A configuration object loaded from file.

    Returns
    -------
    parsed_config : `dict`
        A dictionary of parsed configuration values.
    """

    parsed_config = {}

    for key, child in config.items():
        parsed_config[key] = {}
        for child_key, value in child.items():
            parsed_config[key][child_key] = eval(value)

    return parsed_config
