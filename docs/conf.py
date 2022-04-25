# -*- coding: utf-8 -*-
import os
import sys
sys.path.insert(0, os.path.abspath('..'))

import rejected  # noqa E402

release = rejected.__version__
version = '.'.join(release.split('.')[0:1])

extensions = [
    'sphinx.ext.autodoc', 'sphinx.ext.doctest', 'sphinx.ext.viewcode',
    'sphinx.ext.autosummary', 'sphinx.ext.intersphinx'
]

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'

project = 'rejected'
copyright = '2009-2022, Gavin M. Roy'

exclude_patterns = ['_build']
pygments_style = 'sphinx'

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'pika': ('https://pika.readthedocs.io/en/latest/', None),
    'raven': ('https://raven.readthedocs.io/en/latest/', None),
    'tornado': ('http://www.tornadoweb.org/en/latest/', None)
}

html_theme = 'default'
html_static_path = ['_static']
htmlhelp_basename = 'rejecteddoc'

latex_elements = {}

latex_documents = [
    ('index', 'rejected.tex', u'rejected Documentation', u'Gavin M. Roy',
     'manual'),
]

man_pages = [('index', 'rejected', u'rejected Documentation',
              [u'Gavin M. Roy'], 1)]

texinfo_documents = [
    ('index', 'rejected', u'rejected Documentation', u'Gavin M. Roy',
     'rejected', 'One line description of project.', 'Miscellaneous'),
]
