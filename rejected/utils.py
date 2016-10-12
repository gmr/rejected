import pkg_resources
import sys


def get_module_data():
    modules = {}
    for module_name in sys.modules.keys():
        module = sys.modules[module_name]
        if hasattr(module, '__version__'):
            modules[module_name] = module.__version__
        elif hasattr(module, 'version'):
            modules[module_name] = module.version
        else:
            try:
                version = get_version(module_name)
                if version:
                    modules[module_name] = version
            except Exception:
                pass
    return modules


def get_version(module_name):
    try:
        return pkg_resources.get_distribution(module_name).version
    except pkg_resources.DistributionNotFound:
        return None
