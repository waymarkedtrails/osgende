# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of osgende.
# Copyright (C) 2012 Sarah Hoffmann

from distutils.core import setup

setup(name='osgende',
      version='2.0',
      author='Sarah Hoffmann',
      author_email='lonvia@denofr.de',
      url='https://github.com/waymarkedtrails/osgende',
      packages=['osgende',
                'osgende.common',
                'osgende.common.sqlalchemy',
                'osgende.generic',
                'osgende.lines',
                'osgende.relations',
                'osgende.tools'
               ],
      scripts=['tools/osgende-import', 'tools/osgende-mapgen', 'tools/osgende-mapserv',
               'tools/osgende-mapserv-falcon.py'],
      license='GPL 3.0',
      keywords=["OSM", "OpenStreetMap", "Databases"],
      python_requires = ">=3.7"
      )
