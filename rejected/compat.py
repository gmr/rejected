"""
Compatibility functions for Legacy rejected configuration support

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-22'

import logging

_LOGGER = logging.getLogger('rejected.compat')


def get_compatible_config(config, key, compatible_key=None, default_value=None):
    """Retrieve a key from the configuration passing a default value and
    a compatible key for legacy support. If the correct key does not exist
    check to see if the compatible key does, and warn if it does.

    :param config: Configuration values
    :type config: dict
    :param key: The key to get the value for
    :type key: str
    :param compatible_key: The fallback key
    :type compatible_key: str
    :param default_value: The default value
    :type default_value: any
    :returns: any

    """
    if key not in config:
        if compatible_key and compatible_key in config:
            _LOGGER.warn("Returning deprecated configuration key: %s",
                        compatible_key)
            return config.get(compatible_key, default_value)
    return config.get(key, default_value)


def get_consumer_config(config):
    """Retrieve a compatibility view of the configuration allowing for the
    Bindings legacy top level attribute instead of the now used Consumers top
    level attribute. If the sub-attribute for consumers is present, move all of
    the attributes for it to the same level as the consumers attribute.

    :param config: The configuration as specified in the YAML file
    :type config: dict
    :returns: dict
    :raises: DeprecationWarning

    """
    config = get_compatible_config(config, 'Consumers', 'Bindings')

    # Squash the configs down for compatibility
    for name in config:
        if 'consumers' in config[name]:
            config[name].update(config[name]['consumers'])
            del config[name]['consumers']
            _LOGGER.warn("Consumers attributes automatically moved to same\
 tree-depth as the consumers attribute itself.")

    # Return the config sub-section
    return config


def get_poll_interval(config):
    """Retrieves the poll level attribute from the configuration allowing for
    the legacy Monitoring->interval legacy attribute to be used if is used.

    :param config: The configuration as specified in the YAML file
    :type config: dict
    :returns: int or None
    :raises: DeprecationWarning

    """
    legacy_value = config.get('Monitoring', dict()).get('interval', None)
    if legacy_value:
        logging.warning("Monitoring->interval retrieved but will  be removed\
 from support in favor of the  top level poll_interval attribute.")

    return config.get('poll_interval', )

def is_monitoring_enabled(config):
    """Retrieves the current value in the configuration for the monitor bool.
    If it is not found it will look for the legacy Monitoring node and if set
    will raise a DeprecationWarning and return the value Monitoring->enabled
    with a default of False

    :param config: The configuration as specified in the YAML file
    :type config: dict
    :returns: bool
    :raises: DeprecationWarning

    """

    # We now want to just enable/disable this
    if config.get('monitor') in [True, False]:
        return config['monitor']

    # No legacy monitoring node == no
    if not config.get('Monitoring'):
        return False

    _LOGGER.warning("Top-level attribute of Monitoring found. This has been\
 deprecated in favor of a single top level attribute of monitor: bool")

    # Legacy had it as a sub-variable with interval
    return config['Monitoring'].get('enabled', False)
