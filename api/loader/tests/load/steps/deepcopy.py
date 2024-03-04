import behave
import os
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
    ))

from api.loader import LoadManager

@behave.given("a machine: {machine}, a suite: {suite} and a load: {name}")
def given_impl(context, machine, suite, name):
    if suite == "empty":
        suite = None
    lm = LoadManager(machine=machine, suite=suite)
    lm.import_from_db(username="admin", password="admin", dbname="storehouse")
    load = lm(name)
    context.load = load

@behave.when("we ask for a deepcopy of the load")
def when_impl(context):
    context.deepcopy = context.load.deepcopy()

@behave.then("we get a true deepcopy instance")
def then_impl(context):
    assert (context.deepcopy == context.load) and\
            (id(context.deepcopy) != id(context.load))

