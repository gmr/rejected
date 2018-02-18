# -*- coding: utf-8 -*-
import datetime
import os
import sys

sys.path.insert(0, os.path.abspath('..'))

extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.viewcode',
              'sphinx.ext.autosectionlabel',
              'sphinx.ext.autosummary',
              'sphinx.ext.intersphinx']

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'

project = 'rejected'
copyright = '2009-{}, Gavin M. Roy'.format(
    datetime.date.today().strftime('%Y'))

import rejected
release = rejected.__version__
version = '.'.join(release.split('.')[0:1])

exclude_patterns = ['_build']
pygments_style = 'sphinx'
add_function_parentheses = False

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'pika': ('https://pika.readthedocs.io/en/latest/', None),
    'raven': ('https://raven.readthedocs.io/en/latest/', None),
    'tornado': ('http://www.tornadoweb.org/en/latest/', None)
}

html_theme = 'sphinx_rtd_theme'
