# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende.
# Copyright (C) 2022 Sarah Hoffmann
import re
from urllib.parse import urlparse, quote

UNIT_RE = re.compile(r"\s*(\d+)([.,](\d+))?\s*([a-zA-Z']*)")

# conversion matrix for units of length
LENGTH_MATRIX = {'km' : {'m': 0.001, 'mi': 1.6093,
                         'ft': 0.0003048, "'": 0.0003048},
                 'm' : {'km': 1000.0,
                        'mi': 1609.3, 'ft': 0.3048, "'": 0.3048},
                 'ft' : {'m': 3.28084, 'km': 3280.84,
                         'mi': 5280.0, "'": 1.0},
                 'mi' : {'m': 0.0006213712, 'km': 0.6213712,
                         'ft': 0.0001893939, "'": 0.0001893939}
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

           `locales` must be a list of language codes with
           decreasing preference.
        """
        ret = TagStore()
        tagweights = {}
        for k, v in tags.items():
            parts = k.split(':', 1)
            if len(parts) == 2 and parts[1] in locales:
                weight = locales.index(parts[1])
                if weight < tagweights.get(parts[0], 1000):
                    ret[parts[0]] = v
                    tagweights[parts[0]] = weight
            else:
                if k not in tagweights:
                    ret[k] = v

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

    def get_prefixed(self, prefix):
        """ Return a dictionary of all tags whose keys start with prefix.
            The prefix is removed before putting them into the dict.
        """
        return {k[len(prefix):]: v for k, v in self.items() if k.startswith(prefix)}

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


    def get_url(self, schemes=None, keys=None):
        """Return a properly encoded URL for the object.
           Supports `website` and `url` tags.
        """
        if keys:
            ret = self.firstof(*keys)
        else:
            ret = self.firstof('url', 'website')

        if ret is not None:
            # paranoia, to avoid HTML injection
            ret = ret.replace('"', '%22')
            ret = ret.replace("'", '%27')
            try:
                url = urlparse(ret)
            except ValueError:
                return None
            if not(url.scheme in (schemes or ('http', 'https')) and url.netloc):
                return None

        return ret

    def get_length(self, *tags, unit='km', default='km'):
        """ Return a tag as a distance using the given unit.
            If tags is an iterable, the first tag given is used.

            `default` denotes the unit to use when the tag has
            no unit stated.
        """
        if unit not in LENGTH_MATRIX:
            raise ValueError('Unknown distance unit')

        tag = self.firstof(*tags)
        if tag is None:
            return None

        m = UNIT_RE.match(tag)
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
            if tagunit in LENGTH_MATRIX[unit]:
                return mag * LENGTH_MATRIX[unit][tagunit]

        return None
