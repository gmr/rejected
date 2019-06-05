# coding=utf-8
from os import path

import setuptools


def read_requirements(name):
    """Read in the requirements file and return the list of things to return.

    :param str name: The file to return
    :rtype: list

    """
    requirements = []
    with open(path.join('requires', name)) as req_file:
        for line in req_file:
            if '#' in line:
                line = line[:line.index('#')]
            line = line.strip()
            if line.startswith('-r'):
                requirements.extend(read_requirements(line[2:].strip()))
            elif line and not line.startswith('-'):
                requirements.append(line)
    return requirements


setuptools.setup(
    name='rejected',
    version='3.20.4',
    description='Rejected is a Python RabbitMQ Consumer Framework and '
                'Controller Daemon',
    long_description=open('README.rst').read(),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'License :: OSI Approved :: BSD License'
    ],
    keywords='amqp rabbitmq',
    author='Gavin M. Roy',
    author_email='gavinmroy@gmail.com',
    url='https://github.com/gmr/rejected',
    license='BSD',
    packages=['rejected'],
    package_data={'': ['LICENSE', 'README.rst']},
    include_package_data=True,
    install_requires=read_requirements('installation.txt'),
    extras_require={
        'html': ['beautifulsoup4'],
        'influxdb': ['sprockets-influxdb'],
        'msgpack': ['u-msgpack-python'],
        'sentry': ['raven'],
        'testing': read_requirements('development.txt')
    },
    tests_require=read_requirements('testing.txt'),
    entry_points={'console_scripts': ['rejected=rejected.controller:main']},
    zip_safe=True)
