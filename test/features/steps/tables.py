import logging
from behave import *
from nose.tools import *

@then("table {name} consists of")
def step_impl(context, name):
    exp = set()
    for r in context.table:
        exp.add(tuple([None if r[k] == '~~~' else r[k] for k in context.table.headings]))
    with context.engine.begin() as conn:
        res = conn.execute(context.tables[name].data.select())
        for r in res:
            row = tuple([None if r[k] is None else str(r[k]) for k in context.table.headings])
            assert_in(row, exp)
            exp.remove(row)
        eq_(0, len(exp))
