#!/usr/bin/python
# This file is part of Osgende
# Copyright (C) 2012 Sarah Hoffmann
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

from distutils.core import setup
setup(name='osgende',
      version='0.2',
      author='Sarah Hoffmann',
      author_email='lonvia@denofr.de',
      url='https://github.com/lonvia/osgende',
      packages=['osgende', 'osgende.common', 'osgende.nodes', 'osgende.relations', 'osgende.ways'],
      scripts=['tools/osgende-import', 'tools/osgende-mapgen'],
      )
