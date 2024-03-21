import inspect
from importlib import import_module, reload
from glob import glob
from time import sleep
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))

from realsim.scheduler.scheduler import Scheduler


schedulers_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../realsim/scheduler"
))

stored_modules: dict[str, dict] = dict()

while 1:

    # Get all py files in directory
    py_files = glob("**/*.py", root_dir=schedulers_path, recursive=True)

    # Get files of imported scheduler modules that we stored
    stored_files = [mod_dict["abspath"] 
                    for _, mod_dict in stored_modules.items()]

    # Get all the new module files
    new_files = [new_file for new_file in py_files 
                 if f"{schedulers_path}/{new_file}" not in stored_files]    

    # Import new modules if any
    if new_files != []:

        modules_names = list(map(
            lambda filepath:
            f"realsim.scheduler.{filepath.replace('/', '.').replace('.py', '')}",
            new_files
        ))

        # Dynamic module loading
        dynamic_modules = []
        for mod_name in modules_names:
            dynamic_modules.append(import_module(mod_name))

        for relpath, module in zip(new_files, dynamic_modules):

            stored_modules[module.__name__] = {
                    "abspath": f"{schedulers_path}/{relpath}",
                    "lastmod": os.path.getmtime(
                        f"{schedulers_path}/{relpath}"),
                    "classobj": None,
                    "viewable": False
            }

            for class_name, class_obj in inspect.getmembers(module, inspect.isclass):

                # We only care about subclasses of Scheduler
                if issubclass(class_obj, Scheduler):

                    # If abstract not viewable from dashboard
                    if inspect.isabstract(class_obj):
                        stored_modules[module.__name__]["classobj"] = class_obj,
                    # If concrete viewable from dashboard
                    else:
                        stored_modules[module.__name__]["classobj"] = class_obj,
                        stored_modules[module.__name__]["viewable"] = True,

    print([mod_dict["classobj"][0].name for _, mod_dict in stored_modules.items()
           if mod_dict["classobj"] is not None])

    sleep(4)
