# This file is part of Osgende
# Copyright (C) 2011-2012 Sarah Hoffmann
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

import re
from urllib.parse import urlparse, quote

unit_re = re.compile("\s*(\d+)([.,](\d+))?\s*([a-zA-Z]*)")

# conversion matrix for units of length
length_matrix = { 'km' : { 'm' : 0.001,
                           'mi' : 1.6093 },
                  'm' : { 'km' : 1000,
                          'mi' : 1609.3 }
                }

class TagStore(dict):
    """Hash table for OSM tags that allows various forms of formatting.

       Initialized with a hash of tags.
    """

    def __init__(self, *args):
        dict.__init__(self, *args)

    @staticmethod
    def make_localized(tags, locales):
        """Returns a TagStore with localization replacements.

           locales must be a list of language codes with
           decreasing preference.
        """
        ret = TagStore()
        tagweights = {}
        for k, v in tags.items():
            idx = k.find(':')
            lang = k[idx+1:]
            if idx > 0 and lang in locales:
                w = locales.index(k[idx+1:])
                outkey = k[:idx]
                if w < tagweights.get(outkey, 1000):
                    ret[outkey] = v
                    tagweights[outkey] = w
            else:
                ret[k] = v
                tagweights[k] = 1000

        return ret


    def firstof(self, *tags, default=None):
        """ Return the first tag value for which an entry
            exists in the tags store.
        """
        for t in tags:
            val = self.get(t)
            if val is not None:
                return val

        return default

    def get_booleans(self):
        """Return subset of tags that represent booleans and return
           them as a normalized dict. The subset contains
           all tags that are set to positive boolean (yes, true)
           and all that are set to negative boolean (no, false).
        """
        ret = {}
        for k, v in self.items():
            lowv = v.lower()
            if lowv in ("yes", "true"):
                ret[k] = True
            elif lowv in ("no", "false"):
                ret[k] = False

        return ret


    def get_wikipedia_url(self):
        """Return a link to the wikipedia page for the object.
           Supports tags of the following formats:
           * wikipedia=<page>  (assumes English wikipedia)
           * wikipedia=<lang>:<page>
           * wikipedia:<lang>=<page>
        """
        WIKI_PATTERN = 'https://{}.wikipedia.org/wiki/{}'
        entry = self.get('wikipedia')
        if entry is not None:
            parts = entry.split(':', 1)
            if len(parts) == 1:
                return WIKI_PATTERN.format('en', quote(entry))
            if len(parts[0]) in (2, 3):
                return WIKI_PATTERN.format(parts[0], quote(parts[1]))

        # Try language-specific tags
        for k, v in self.items():
            if k.startswith('wikipedia:') and len(k) in (12, 13) \
               and not v.startswith('http'):
                return WIKI_PATTERN.format(k[10:], quote(v))

        return None


    def get_wikipedia_tags(self):
        """Return a dictionary of available wikipedia links.
           Supports tags of the following formats:
           * wikipedia=<page>  (assumes English wikipedia)
           * wikipedia=<lang>:<page>
           * wikipedia:<lang>=<page>

           where <page> may either be just the page name or the
           complete url. Page names are not extended to url format.

           Returns an empty dictionary if the object has no wikipedia tags.
        """
        ret = {}
        for k, v in self.items():
            if k == 'wikipedia':
                parts = v.split(':', 1)
                if len(parts) == 1:
                    ret['en'] = v
                if len(parts[0]) in (2, 3):
                    ret[parts[0]] = parts[1]
            elif k.startswith('wikipedia:') and len(k) in (12, 13) \
                 and not v.startswith('http'):
                ret[k[10:]] = v

        return ret


    def get_url(self, schemas=None):
        """Return a properly encoded URL for the object.
           Supports `website` and `url` tags.
        """
        ret = self.firstof('url', 'website')

        if ret is not None:
            # paranoia, to avoid HTML injection
            ret.replace('"', '%22')
            ret.replace("'", '%27')
            try:
                url = urlparse(ret)
            except ValueError:
                return None
            if not(url.schema in (schemas or ('http', 'https')) and url.netloc):
                return None

        return ret

    def get_length(self, *tags, unit='km', default='km'):
        """ Return a tag as a distance using the given unit.
            If tags is an iterable, the first tag given is used.

            `default` denotes the unit to use when the tag has
            no unit stated.
        """
        if unit not in length_matrix:
            raise Error('Unknown distance unit')

        tag = self.firstof(*tags)
        if tag is None:
            return None

        m = unit_re.match(tag)
        if m is not None:
            if m.group(3) is None:
                mag = float(m.group(1))
            else:
                mag = float('%s.%s' % (m.group(1), m.group(3)))
            tagunit = m.group(4).lower()
            if tagunit == '':
                tagunit = default
            if tagunit == unit:
                return mag
            elif tagunit in length_matrix[unit]:
                return mag * length_matrix[unit][tagunit]

        return None
