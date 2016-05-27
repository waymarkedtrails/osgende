import logging
from behave import *
from nose.tools import *
from geoalchemy2.elements import WKBElement
from geoalchemy2.shape import to_shape
from shapely.geometry import Point, LineString, MultiLineString

logger = logging.getLogger(__name__)

def table_row_to_tuple(row, headings):
    out = []
    for col in headings:
        assert_in(col, row)
        if row[col] is None:
            out.append(None)
        elif isinstance(row[col], list):
            out.append(','.join([str(x) for x in row[col]]))
        elif isinstance(row[col], WKBElement):
            geom = to_shape(row[col])
            if isinstance(geom, Point):
                out.append("%s %s" % (geom.x, geom.y))
            elif isinstance(geom, LineString):
                out.append(", ".join(["%s %s" % p for p in geom.coords]))
            elif isinstance(geom, MultiLineString):
                out.append(", ".join(['(' +
                            ", ".join(["%s %s" % p for p in l.coords]) + ')'
                            for l in geom]))
            else:
                assert_false("Unknown geometry type %s" % type(geom))
        else:
            out.append(str(row[col]))
    return tuple(out)

@then("table {name} consists of")
def step_impl(context, name):
    exp = set()
    for r in context.table:
        exp.add(tuple([None if r[k] == '~~~' else r[k] for k in context.table.headings]))
    with context.engine.begin() as conn:
        res = conn.execute(context.tables[name].data.select())
        for r in res:
            eq_(len(context.table.headings), len(r), "Unexpected number of columns")
            row = table_row_to_tuple(r, context.table.headings)
            assert_in(row, exp)
            exp.remove(row)
    eq_(0, len(exp), "Missing rows in table: %s" % str(exp))

@then("table {name} consists of rows")
def step_impl(context, name):
    exp = set()
    for r in context.table:
        exp.add(tuple([None if r[k] == '~~~' else r[k] for k in context.table.headings]))
    with context.engine.begin() as conn:
        res = conn.execute(context.tables[name].data.select())
        for r in res:
            row = table_row_to_tuple(r, context.table.headings)
            assert_in(row, exp)
            exp.remove(row)
    eq_(0, len(exp), "Missing rows in table: %s" % str(exp))


@when("updating table {name}")
def step_impl(context, name):
    context.tables[name].update(context.engine)

