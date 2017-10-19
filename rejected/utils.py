import importlib
import math
import pkg_resources


def get_package_version(module_obj, value):
    """Get the version of a package or a module's package.

    :param object module_obj: The module that was imported for the consumer
    :param str value: The namespaced module path or package name
    :rtype: str or None

    """
    for key in ['version', '__version__']:
        if hasattr(module_obj, key):
            return getattr(module_obj, key)
    parts = value.split('.')
    for index, part in enumerate(parts):
        try:
            return pkg_resources.get_distribution(
                '.'.join(parts[0:index + 1])).version
        except (pkg_resources.DistributionNotFound,
                pkg_resources.RequirementParseError):
            continue


def import_consumer(value):
    """Pass in a string in the format of foo.Bar, foo.bar.Baz, foo.bar.baz.Qux
    and it will return a handle to the class, and the version.

    :param str value: The consumer class in module.Consumer format
    :return: tuple(Class, str)

    """
    parts = value.split('.')
    module_obj = importlib.import_module('.'.join(parts[0:-1]))
    return (getattr(module_obj, parts[-1]),
            get_package_version(module_obj, value))


def percentile(values, k):
    """Find the percentile of a list of values.

    :param list values: The list of values to find the percentile of
    :param int k: The percentile to find
    :rtype: float or int

    """
    if not values:
        return None
    values.sort()
    index = (len(values) * (float(k) / 100)) - 1
    return values[int(math.ceil(index))]
