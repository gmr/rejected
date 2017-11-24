from setuptools import setup


def read_requirements_file(name):
    reqs = []
    try:
        with open(name) as req_file:
            for line in req_file:
                if '#' in line:
                    line = line[0:line.index('#')]
                line = line.strip()
                if line:
                    reqs.append(line)
    except IOError:
        pass
    return reqs


setup(name='rejected',
      version='4.0.0',
      description='Rejected is a Python RabbitMQ Consumer Framework and '
                  'Controller Daemon',
      long_description=open('README.rst').read(),
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
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
      install_requires=read_requirements_file('requires/installation.txt'),
      extras_require={
          'html': ['beautifulsoup4'],
          'influxdb': ['sprockets-influxdb'],
          'msgpack': ['u-msgpack-python'],
          'sentry': ['raven']
      },
      tests_require=read_requirements_file('requires/development.txt'),
      entry_points=dict(console_scripts=['rejected=rejected.controller:main']),
      zip_safe=True)
