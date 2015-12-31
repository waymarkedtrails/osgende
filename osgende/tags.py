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
import urllib

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
        for k,v in tags.items():
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
        for k,v in self.items():
            lowv = v.lower()
            if lowv in ("yes", "true"):
                ret[k] = True
            elif lowv in ("no", "false"):
                ret[k] = False

        return ret


    def get_wikipedia_url(self, as_url=True):
        """Return a link to the wikipedia page for the object.
           Supports tags of the following formats:
           * wikipedia=<page>  (assumes English wikipedia)
           * wikipedia=<lang>:<page>
           * wikipedia:<lang>=<page>

           If `as_url` is true, then <page> may be either a
           complete URL or a page name. If it set to false, URLs
           in <page> are ignored.
        """
        entry = None # triple of weight, language, link
        if 'wikipedia' in self:
            v = self['wikipedia']
            idx = v.find(':')
            if idx in (2, 3):
                entry = (v[:idx], v[idx+1:])
            else:
                entry = ('en', v)

        if ret is None or (not as_url and ret[1].startswith('http')):
            for k,v in self.items():
                if k.startswith('wikipedia:') and (as_url or not v.startswith('http')):
                    entry = (k[10:], v)
                    break
            else:
                return None

        # paranoia, avoid HTML injection
        ret[1].replace('"', '%22')
        ret[1].replace("'", '%27')
        if ret[1].startswith('http'):
            return ret[1] if as_url else None

        return 'http://%s.wikipedia.org/wiki/%s' % ret

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
        for k,v in self.items():
            if k == 'wikipedia':
                idx = v.find(':')
                if idx in (2, 3):
                    if not v[idx+1:].startswith('http'):
                        ret[v[:idx]] = v[idx+1:]
                else:
                    if not v.startswith('http'):
                        ret['en'] = v
            elif k.startswith('wikipedia:') and not v.startswith('http'):
                ret[k[10:]] = v

        return ret


    def get_url(self):
        """Return a properly encoded URL for the object.
           Supports `website` and `url` tags, with and without protocol prefix.
        """
        ret = self.firstof('url', 'website')

        if ret is not None:
            # paranoia, to avoid HTML injection
            ret.replace('"', '%22')
            ret.replace("'", '%27')
            proto = ret.find(':')
            if proto < 0:
                ret = 'http://%s' % ret

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
