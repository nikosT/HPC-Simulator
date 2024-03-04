import behave
import os
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
    ))

from api.loader import LoadManager

@behave.given("two load managers which have loads of different machines")
def given_impl(context):
    lm1 = LoadManager("aris.compute", "NAS")
    lm1.import_from_db(username="admin", password="admin", dbname="storehouse")
    lm2 = LoadManager("marconi", "NAS")
    lm2.import_from_db(username="admin", password="admin", dbname="storehouse")

    context.lm1 = lm1
    context.lm2 = lm2

@behave.then("we get an empty load manager")
def then_impl(context):
    assert context.lm3.loads == dict()
