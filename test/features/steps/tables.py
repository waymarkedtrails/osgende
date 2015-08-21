import logging
from behave import *
from nose.tools import *
from geoalchemy2.elements import WKBElement
from geoalchemy2.shape import to_shape
from shapely.geometry import Point

def table_row_to_tuple(row, headings):
    out = []
    for col in headings:
        assert_in(col, row)
        if row[col] is None:
            out.append(None)
        elif isinstance(row[col], WKBElement):
            geom = to_shape(row[col])
            if isinstance(geom, Point):
                out.append("%s %s" % (geom.x, geom.y))
            else:
                assert_false("Unknown geometry type")
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
            eq_(len(context.table.headings), len(r))
            row = table_row_to_tuple(r, context.table.headings)
            assert_in(row, exp)
            exp.remove(row)
        eq_(0, len(exp))


@when("updating table {name}")
def step_impl(context, name):
    context.tables[name].update(context.engine)

