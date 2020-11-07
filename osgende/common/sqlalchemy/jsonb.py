# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2017-2020 Sarah Hoffmann

# With minor modifications borrowed from
# https://bitbucket.org/zzzeek/sqlalchemy/issues/3566/figure-out-how-to-support-all-of-pgs

from sqlalchemy.dialects.postgresql import JSONB
from .column_function import ColumnFunction

class jsonb_array_elements(ColumnFunction):
    name = 'jsonb_array_elements'
    column_names = [('value', JSONB)]
