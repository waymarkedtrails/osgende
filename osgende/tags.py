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
                }

class TagStore(dict):
    """Hash table for OSM tags that allows various forms of formatting.

       Initialized with a hash of tags.
    """

    def __init__(self, *args):
        dict.__init__(self, *args)

    def get_localized_tagstore(self, locales):
        """Returns a TagStore with localization replacements.

           locales must be a hash where the keys are language
           codes and the values are weights. Larger weights are
           preferred.
        """
        ret = TagStore()
        tagweights = {}
        for k,v in self.items():
            idx = k.find(':')
            if idx > 0 and k[idx+1:] in locales:
                outkey = k[:idx]
                w = locales[k[idx+1:]]
                if outkey not in ret or w > tagweights[outkey]:
                    ret[outkey] = v
                    tagweights[outkey] = w
            else:
                if k not in ret:
                    ret[k] = v
                    tagweights[k] = -1000.0
        return ret

    def get_firstof(self, tags, default=None):
        """ Return the first tag value for which an entry
            exists in the tags store.
        """

        if isinstance(tags, str):
            return self.get(tags, default)

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


    def get_wikipedia_url(self, locales=None):
        """Return a link to the wikipedia page for the object.
           Supports tags of the following formats:
           * wikipedia=<page>  (assumes English wikipedia)
           * wikipedia=<lang>:<page>
           * wikipedia:<lang>=<page>

           where <page> may either be just the page name or the
           complete url.

           locales allows to state the preferred language
           if multiple tags are available.
        """
        ret = None # triple of weight, language, link
        if locales is None:
            locales = {'en' : 1.0}
        for k,v in self.items():
            newurl = None # tuple of languge, link
            if k == 'wikipedia':
                if len(v) > 3 and v[2] == ':':
                    newurl = (v[:2], v[3:])
                else:
                    newurl = ('en', v)
            elif k.startswith('wikipedia:'):
                newurl = (k[10:], v)
            if newurl is not None:
                w = locales.get(newurl[0], 0)
                if ret is None or w > ret[0]:
                    ret = (w, newurl[0], newurl[1])

        if ret is None:
            return None
        else:
            # paranoia, avoid HTML injection
            ret[2].replace('"', '%22')
            ret[2].replace("'", '%27')
            if ret[2].startswith('http:'):
                return ret[2]
            else:
                return 'http://%s.wikipedia.org/wiki/%s' % (ret[1], ret[2])

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
                if len(v) > 3 and v[2] == ':':
                    ret[v[:2]] = v[3:]
                elif len(v) > 4 and v[3] == ':' :
                    ret[v[:3]] = v[4:]
                else:
                    ret['en'] = v
            elif k.startswith('wikipedia:'):
                ret[k[10:]] = v

        return ret


    def get_url(self):
        """Return a properly encoded URL for the object.
           Supports `website` and `url` tags, with and without protocol prefix.
        """
        ret = self.get_firstof('url', 'website')

        if ret is not None:
            # paranoia, to avoid HTML injection
            ret.replace('"', '%22')
            ret.replace("'", '%27')
            proto = ret.find(':')
            if proto < 0:
                ret = 'http://%s' % ret

        return ret

    def get_as_length(self, tags, unit='km', default='km'):
        """ Return a tag as a distance using the given unit.
            If tags is an iterable, the first tag given is used.

            `default` denotes the unit to use when the tag has
            no unit stated.
        """
        if unit not in length_matrix:
            raise Error('Unknown distance unit')

        tag = self.get_firstof(tags)
        val = None

        if tag is not None:
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
                    val = mag
                elif tagunit in length_matrix[unit]:
                    val = mag * length_matrix[unit][tagunit]

        return val
