import inspect
from importlib import import_module, reload, invalidate_caches
from glob import glob
from time import sleep

import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))

from realsim.scheduler.scheduler import Scheduler

schedulers_path = os.path.join(
        os.path.dirname(__file__), "../realsim/scheduler"
)

stored_modules: dict[str, dict[str, str]] = dict() # stored paths for each module
abstract: dict[str, dict] = dict() # abstract scheduler subclasses
concrete: dict[str, dict] = dict() # concrete scheduler subclasses

while 1:

    # Get modules names
    modules_paths = glob("**/*.py", root_dir=schedulers_path, recursive=True)

    # Get all the paths stored in our dictionary
    stored_paths = [m_dict["path"] for _, m_dict in stored_modules.items()]

    # Get new modules' paths if any
    diff_mod_paths = [path for path in modules_paths 
                      if path not in stored_paths]

    # If new module found
    if diff_mod_paths != []:

        modules_names = list(map(lambda path: 
                                 path.replace("/", ".").replace(".py", ""), 
                                 diff_mod_paths
                                 )
                             )

        modules_names = list(map(lambda path: 
        f"realsim.scheduler.{path}", modules_names))

        # Dynamically import new modules
        dyn_modules = []
        for module_name in modules_names:
            dyn_modules.append(import_module(module_name))

        # Store class objects
        for path, mod in zip(diff_mod_paths, dyn_modules):
            for _, class_obj in inspect.getmembers(mod, inspect.isclass):
                if issubclass(class_obj, Scheduler):
                    if inspect.isabstract(class_obj):
                        abstract[class_obj.name] = {
                                "class": class_obj,
                                "path": f"{schedulers_path}/{path}",
                                "last modified":
                                os.path.getmtime(f"{schedulers_path}/{path}")
                        }
                    else:
                        concrete[class_obj.name] = {
                                "class": class_obj,
                                "path": f"{schedulers_path}/{path}",
                                "last modified": 
                                os.path.getmtime(f"{schedulers_path}/{path}")
                        }
                else:
                    underconstruction[mod] = {
                            "path": f"{schedulers_path}/{path}",
                            "last modified": os.path.getmtime(f"{schedulers_path}/{path}")
                    }

    # Check if last modified time of module file is different than the one
    # stored
    for name, class_dict in concrete.items():
        if class_dict["last modified"] != os.path.getmtime(class_dict["path"]):
            module = inspect.getmodule(class_dict["class"])
            # invalidate_caches()
            reload(module)
            for _, class_obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(class_obj, Scheduler):
                    if not inspect.isabstract(class_obj):
                        concrete[name]["class"] = class_obj
                        concrete[name]["last modified"] = os.path.getmtime(
                                concrete[name]["path"]
                        )

    for mod, mod_dict in concrete.items():
        if mod_dict["last modified"] != os.path.getmtime(mod_dict["path"]):
            # invalidate_caches()
            reload(mod)
            for _, class_obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(class_obj, Scheduler):
                    if not inspect.isabstract(class_obj):
                        concrete[name]["class"] = class_obj
                        concrete[name]["last modified"] = os.path.getmtime(
                                concrete[name]["path"]
                        )

    print([class_dict["class"].name for _, class_dict in concrete.items()])

    # Sleep for 10 seconds before checking again
    sleep(10)

exit(0)
for module in modules:
    __import__(module)

for module in modules:
    print(inspect.ismodule(module))
    for member in inspect.getmembers(module, inspect.isclass):
        print(member)
