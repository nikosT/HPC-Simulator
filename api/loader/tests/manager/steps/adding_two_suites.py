import behave
import os
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
    ))

from api.loader import LoadManager

@behave.given("two load managers that manage loads of different suites on the same machine")
def given_impl(context):
    lm1 = LoadManager("aris.compute", "NAS")
    lm1.import_from_db(username="admin", password="admin", dbname="storehouse")
    lm2 = LoadManager("aris.compute", "SPEC")
    lm2.import_from_db(username="admin", password="admin", dbname="storehouse")

    context.lm1 = lm1
    context.lm2 = lm2

@behave.when("we add the two")
def when_impl(context):
    context.lm3 = context.lm1 + context.lm2

@behave.then("we get a new load manager with all the loads of both managers on the same machine")
def then_impl(context):
    for name, _ in context.lm3:
        assert name in context.lm1.loads or name in context.lm2.loads
