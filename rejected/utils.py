import importlib
import importlib.metadata
import math
import types
import typing


def get_package_version(
    module_obj: types.ModuleType, value: str
) -> str | None:
    """Get the version of a package or a module's package.

    :param module_obj: The module that was imported for the consumer
    :param value: The namespaced module path or package name

    """
    for key in ['version', '__version__']:
        if hasattr(module_obj, key):
            return str(getattr(module_obj, key))
    parts = value.split('.')
    for index, _part in enumerate(parts):
        try:
            return importlib.metadata.version('.'.join(parts[0 : index + 1]))
        except importlib.metadata.PackageNotFoundError:
            continue
    return None


def import_consumer(value: str) -> tuple[typing.Any, str | None]:
    """Pass in a string in the format of foo.Bar, foo.bar.Baz,
    foo.bar.baz.Qux and it will return a handle to the class, and
    the version.

    :param value: The consumer class in module.Consumer format

    """
    parts = value.split('.')
    module_obj = importlib.import_module('.'.join(parts[0:-1]))
    return (
        getattr(module_obj, parts[-1]),
        get_package_version(module_obj, value),
    )


def percentile(values: list[float], k: int) -> float | None:
    """Find the percentile of a list of values.

    :param values: The list of values to find the percentile of
    :param k: The percentile to find

    """
    if not values:
        return None
    values.sort()
    index = (len(values) * (float(k) / 100)) - 1
    return values[math.ceil(index)]
