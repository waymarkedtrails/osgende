# This file is part of Osgende
# Copyright (C) 2011 Sarah Hoffmann
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
"""
Simple importer of OSM planet dumps and diffs into a simplified Osmosis
style postgresql database.

Diffs must be applied in correct order in order to keep database integrity.
However, diffs can be savely reapplied, i.e. it is possible to reapply an
older diff iff all diffs that follow are reapplied as well.

Diffs must not contain duplicates. Use osmosis' simplifyChange to remove
duplicates.
"""

import sys
import codecs
import tempfile
import struct
import xml.parsers.expat
from optparse import OptionParser

import osgende.common.postgisconn as postgisconn

class DbDumper:
    tempdir = '.'
    def __init__(self, db, table, columns):
        self.db = db
        self.table = table
        self.dumpfile = codecs.getwriter('utf-8')(
                tempfile.NamedTemporaryFile(prefix=table,dir=DbDumper.tempdir))
        self.counter = 0
        self.columns = columns
        self.linepattern = '\t'.join([u"%%(%s)s" % x for x in columns]) + '\n'
        if columns[0] == 'id':
            self.updatequery = "EXECUTE dump_%s_update(%s)" % (
                            table, ','.join(['%s' for x in columns]))
            db.prepare("dump_%s_update" % table,
                       "UPDATE %s SET (%s) = (%s) WHERE id = $1" % (
                            table, ','.join(columns[1:]),
                            ','.join(['$%d' % x for x in range(2,len(columns)+1)])))
        #print "Linepattern:",self.linepattern

    def write(self, attrs):
        self.dumpfile.write(self.linepattern % attrs)
        self.counter += 1
        if self.counter > DbDumper.maxentries:
            self.flush()
            self.counter = 0

    def flush(self):
        cur = self.db.cursor()
        self.dumpfile.flush()
        self.dumpfile.seek(0)
        cur.copy_from(self.dumpfile, self.table, null='NULL', columns=self.columns)
        self.dumpfile.seek(0)
        self.dumpfile.truncate()

    def update(self, attrs):
        #print cursor.mogrify(self.updatequery, [attrs[x] for x in self.columns] + [attrs['id']])
        cur = self.db.cursor()
        cur.execute(self.updatequery, [attrs[x] for x in self.columns])
        return (cur.rowcount == 1)


class OSMImporter:
    columns = {
        'nodes': ('id', 'tags', 'geom' ),
        'ways': ('id', 'tags', 'nodes'),
        'relations' : ('id', 'tags'),
        'relation_members' : ( 'relation_id', 'member_id', 'member_type',
                               'member_role', 'sequence_id' )
    }

    osm_types = ('node', 'way', 'relation')


    def __init__(self, options):
        dba = ('dbname=%s user=%s password=%s' %
               (options.database, options.username, options.password))
        self.db = postgisconn.PGDatabase(dba)
        self.cursor = self.db.cursor()

        DbDumper.maxentries = options.maxentries
        DbDumper.tempdir = options.tempdir
        self.dumpers = {}
        for (tab, cols) in OSMImporter.columns.iteritems():
            self.dumpers[tab] = DbDumper(self.db, tab, cols)

    def readfile(self, filename):
        parser = xml.parsers.expat.ParserCreate()
        parser.StartElementHandler = self.parse_start_element
        parser.EndElementHandler = self.parse_end_element
        self.handler = self.handle_initial_state

        if filename == '-':
            fid = sys.stdin
        else:
            fid = open(filename,'r')
        parser.ParseFile(fid)
        fid.close()
        for tab in self.dumpers.itervalues():
            tab.flush()
        self.db.commit()

    def parse_start_element(self, name, attrs):
        self.handler(True, name, attrs)

    def parse_end_element(self, name):
        self.handler(False, name, {})

    def handle_initial_state(self, start, name, attrs):
        if start and name in ('osm','osmChange'):
            if 'version' not in attrs:
                raise Exception('Not a valid OSM file. Version information missing.')
            if attrs['version'] != '0.6':
                raise Exception("OSM file is of version %s. Can only handle version 0.6 files."
                                  % attrs['version'])

            self.handler = self.handle_top_level
            self.intag = None
            self.filetype = name
            self.action = None
            if name == 'osmChange':
                for tab in OSMImporter.osm_types:
                    tablename = '%s_changeset' % tab
                    if tab == 'node':
                        self.dumpers[tablename] = DbDumper(self.db, tablename,
                                                   ('id', 'action', 'tags', 'geom'))
                    else:
                        self.dumpers[tablename] = DbDumper(self.db, tablename,
                                                   ('id', 'action'))
        else:
            raise Exception("Not an OSM file.")

    def handle_top_level(self, start, name, attrs):
        #print "Top level:",name,attrs,start
        if start and name in OSMImporter.osm_types:
            if not 'id' in attrs:
                raise Exception('%s has no id.' % name)
            self.current = {}
            self.current['type'] = name
            self.current['id'] = attrs['id']
            self.current['tags'] = {}
            self.memberseq = 1
            if self.filetype == 'osmChange':
                if self.action is None:
                    raise Exception('Missing change type for %s %s.' % (name, attrs['id']))
                self.prepare_object()
            if name == 'way':
                self.current['nodes'] = []
            elif name == 'node':
                if 'lat' not in attrs or 'lon' not in attrs:
                    raise Exception('Node %s is missing coordinates.' % attrs['id'])
                # WKB representation
                # PostGIS extension that includes a SRID, see postgis/doc/ZMSGeoms.txt
                self.current['geom'] = struct.pack("=biidd", 1, 0x20000001, 4326,
                                        float(attrs['lon']), float(attrs['lat'])).encode('hex')
            self.handler = self.handle_object
        elif start and name == 'bound':
            self.handler = self.handle_bound
        elif start and name == 'changeset':
            self.handler = self.handle_changeset
        elif name in ('modify', 'create', 'delete'):
            if start:
                if self.action is None:
                    self.action = name
                else:
                    raise Exception('Nested change actions.')
            else:
                if self.action == name:
                    self.action = None
                else:
                    raise Exception('Unexpected change action.')

        elif not start and name == self.filetype:
            self.handler = self.handle_eof
        else:
            raise Exception('Unexpected element: %s' % name)

    def handle_bound(self, start, name, attrs):
        if not start and name == 'bound':
            self.handler = self.handle_top_level
        else:
            raise Exception("Unexpected data in bound description.")

    def handle_changeset(self, start, name, attrs):
        if not start and name == 'changeset':
            self.handler = self.handle_top_level

    def handle_object(self, start, name, attrs):
        #print "Object level:",name,attrs,start
        if start:
            if self.intag:
                raise Exception("Closing element %s missing."% (self.intag))
            if name == 'tag':
                if 'k' not in attrs or 'v' not in attrs:
                    raise Exception("tag element of invalid format")
                self.current['tags'][attrs['k']] = attrs['v']
            elif name == 'nd' and 'nodes'in self.current:
                if not 'ref' in attrs or not attrs['ref'].isdigit():
                    print self.current
                    raise Exception("Unexcpected nd element")
                self.current['nodes'].append(attrs['ref'])
            elif name == 'member':
                 self.write_relation_member(attrs)
            else:
                raise Exception("Unexexpected element %s."% name)

            self.intag = name
        else:
            if self.intag:
                if name == self.intag:
                    self.intag = None
                else:
                    raise Exception("Closing element %s missing."% (self.intag))
            elif name == self.current['type']:
                self.write_object()
                self.handler = self.handle_top_level
            else:
                raise Exception("Spurious final tag %s" % name)

    def handle_eof(self, start, name, attrs):
        raise Exception("Data after end of file.")

    sqltrans = { ord(u'"'): u'\\\\"',
                     ord(u'\r') : u' ', ord(u'\b'): None, ord(u'\f') : None,
                     ord(u'\n') : u' ',
                     ord(u'\t') : u' ',
                     ord(u'\\') : u'\\\\\\\\'
               }

    def write_relation_member(self, attrs):
        if self.current['type'] != 'relation':
            raise Exception("member element in a %s" % self.current['type'])

        if not attrs.get('type', None) in OSMImporter.osm_types:
            raise Exception("Missing or unknown type attribute in member element.")
        if not 'ref' in attrs or not attrs['ref'].isdigit():
            raise Exception("Missing attribute ref in member element.")
        if not 'role' in attrs:
            raise Exception("Missing attribute role in member element.")
        self.dumpers['relation_members'].write(
                {'relation_id' : self.current['id'],
                 'member_id': attrs['ref'],
                 'member_type' : attrs['type'][0].upper(),
                 'member_role' : '%s' % attrs['role'].translate(OSMImporter.sqltrans),
                 'sequence_id' : self.memberseq})
        self.memberseq += 1

    def write_object(self):
        # fix the tags string
        oldtags = self.current['tags']
        taglist = ['"%s"=>"%s"' % (k.translate(OSMImporter.sqltrans),
                                   v.translate(OSMImporter.sqltrans))
                   for  (k,v) in self.current['tags'].iteritems()]
        taglist = u','.join(taglist)
        self.current['tags'] = taglist
        if self.action is not None:
            self.current['action'] = self.action[0].upper()
            self.dumpers[self.current['type'] + '_changeset'].write(self.current)
        if self.action == 'delete':
            self.cursor.execute("DELETE FROM %ss WHERE id = %%s" % self.current['type'],
                                  (self.current['id'],))
        else:
            if 'nodes' in self.current:
                strnodes = self.current['nodes']
            if self.action is not None:
                # try to simply update
                self.current['tags'] = oldtags
                if 'nodes' in self.current:
                    self.current['nodes'] = [long(x) for x in self.current['nodes']]
                if self.dumpers[self.current['type']+'s'].update(self.current):
                    return
                self.current['tags'] = taglist

            if 'nodes' in self.current:
                self.current['nodes'] = u'{%s}' % (
                                       ','.join(strnodes))
            self.dumpers[self.current['type']+'s'].write(self.current)

    def prepare_object(self):
        if self.current['type'] == 'relation':
            self.cursor.execute("""DELETE FROM relation_members
                                  WHERE relation_id = %s""", (self.current['id'],))


if __name__ == '__main__':

    # fun with command line options
    parser = OptionParser(description=__doc__,
                          usage='%prog [options] <osm file>')
    parser.add_option('-d', action='store', dest='database', default='osmosis',
                       help='name of database')
    parser.add_option('-u', action='store', dest='username', default='osm',
                       help='database user')
    parser.add_option('-p', action='store', dest='password', default='',
                       help='password for database')
    parser.add_option('-t', action="store", dest='tempdir', default=None,
                       help="directory to use for temporary files")
    parser.add_option('-m', action='store', dest='maxentries', default=100000000,
                       help='Maximum number of objects to cache before writing to the database.')

    (options, args) = parser.parse_args()

    if len(args) == 0:
        OSMImporter(options).readfile('-')
    elif len(args) == 1:
        OSMImporter(options).readfile(args[0])
    else:
        parser.print_help()

