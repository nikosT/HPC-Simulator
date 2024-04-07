import inspect
from importlib import import_module, invalidate_caches, reload
from glob import glob
from time import sleep
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))

from realsim.scheduler.scheduler import Scheduler


def hierarchy(classes):

    class_hierarchy = list()

    while classes != []:
        
        class_obj = classes[0]
        classes.remove(class_obj)

        if class_hierarchy == []:
            class_hierarchy.append(class_obj)
        else:
            inserted = False
            for i in range(len(class_hierarchy)):
                if not issubclass(class_obj, class_hierarchy[i]):
                    class_hierarchy.insert(i, class_obj)
                    inserted = True
                    break

            if not inserted:
                class_hierarchy.append(class_obj)

    return class_hierarchy

def is_subclass(cls, default_cls):
    for cl in inspect.getmro(cls):
        if cl == default_cls:
            return True
    return False

schedulers_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../realsim/scheduler"
))

stored_modules: dict[str, dict] = dict()

while 1:

    invalidate_caches()

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
                    "module": module,
                    "abspath": f"{schedulers_path}/{relpath}",
                    "lastmod": os.path.getmtime(
                        f"{schedulers_path}/{relpath}"),
                    "classobj": None,
                    "viewable": False
            }

            classes = [class_obj 
                       for _, class_obj 
                       in inspect.getmembers(module, inspect.isclass)]

            subclasses = list(
                    filter(lambda cls: issubclass(cls, Scheduler), classes)
            )

            class_hierarchy = hierarchy(subclasses)

            if class_hierarchy != []:
                
                # The last class object is the class defined in the module
                class_obj = class_hierarchy[-1]

                # If abstract not viewable from dashboard
                if inspect.isabstract(class_obj):
                    stored_modules[module.__name__]["classobj"] = class_obj
                # If concrete viewable from dashboard
                else:
                    stored_modules[module.__name__]["classobj"] = class_obj
                    stored_modules[module.__name__]["viewable"] = True

    # Reload all modules if any modification took place in the code of at least
    # one module and remove the modules that were deleted
    code_changed = False
    to_be_removed: list[str] = list()
    for mod_name, mod_dict in stored_modules.items():

        if not os.path.exists(mod_dict["abspath"]):
            to_be_removed.append(mod_name)
            continue

        if os.path.getmtime(mod_dict["abspath"]) != mod_dict["lastmod"]:
            code_changed = True
            print(mod_dict["abspath"])
            break

    for mod_name in to_be_removed:
        stored_modules.pop(mod_name)

    if code_changed:

        for mod_name, mod_dict in stored_modules.items():

            if mod_name == "realsim.scheduler.scheduler":
                continue

            module = reload(mod_dict["module"])
            mod_dict["module"] = module

            classes = [class_obj 
                       for _, class_obj 
                       in inspect.getmembers(module, inspect.isclass)]

            subclasses = list(
                    filter(lambda cls: is_subclass(cls, Scheduler), classes)
            )

            if subclasses == []:
                print(mod_name)
        
            class_hierarchy = hierarchy(subclasses)

            if class_hierarchy != []:

                class_obj = class_hierarchy[-1]

                stored_modules[mod_name]["classobj"] = class_obj
                stored_modules[mod_name]["lastmod"] = os.path.getmtime(stored_modules[mod_name]["abspath"])

    for key, values in stored_modules.items():
        print(f"[{key}]", end="\t")
        print(values["lastmod"], values["classobj"])

    print()

    sleep(4)
