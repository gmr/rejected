"""
common.py

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-22'


def get_consumer_config(config):
    return config.get('Consumers') or config.get('Bindings')

def get_poll_interval(config):
    monitoring = config.get('Monitoring', dict())
    return config.get('poll_interval', monitoring.get('interval', None))

def monitoring_enabled(config):

    # We now want to just enable/disable this
    if config.get('monitor') in [True, False]:
        return config['monitor']

    # No legacy monitoring node == no
    if not config.get('Monitoring'):
        return False

    # Legacy had it as a sub-variable with interval
    return config['Monitoring'].get('enabled', False)
