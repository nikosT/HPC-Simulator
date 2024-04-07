import os
import sys
from importlib import import_module, invalidate_caches, reload
from inspect import getmembers, getmro, isclass, isabstract
from glob import glob

# realsim
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../"
)))

from realsim.cluster.abstract import AbstractCluster
from realsim.generators.abstract import AbstractGenerator
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
    for cl in getmro(cls):
        if cl == default_cls:
            return True
    return False

generators_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../realsim/generators"
))

clusters_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../realsim/cluster"
))

schedulers_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../realsim/scheduler"
))

def update_modules(stored_modules, CLASS):

    changed = False

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

        # Changed by new module, by removing a module, by code change
        changed = True

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
                       in getmembers(module, isclass)]

            subclasses = list(
                    filter(lambda cls: is_subclass(cls, Scheduler), classes)
            )

            class_hierarchy = hierarchy(subclasses)

            if class_hierarchy != []:
                
                # The last class object is the class defined in the module
                class_obj = class_hierarchy[-1]

                # If defined class_obj already exists then store the module with
                # None class object pointing to
                for _, mod_dict in stored_modules.items():
                    if class_obj == mod_dict["classobj"]:
                        mod_dict["classobj"].name += " [Duplicate]"
                        break

                # If abstract not viewable from dashboard
                if isabstract(class_obj):
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
            changed = True
            continue

        if os.path.getmtime(mod_dict["abspath"]) != mod_dict["lastmod"]:
            code_changed = True
            print(mod_dict["abspath"])
            break

    for mod_name in to_be_removed:
        stored_modules.pop(mod_name)

    if code_changed:

        changed = True

        for mod_name, mod_dict in stored_modules.items():

            if mod_name == "realsim.scheduler.scheduler":
                continue

            module = reload(mod_dict["module"])
            mod_dict["module"] = module

            classes = [class_obj 
                       for _, class_obj 
                       in getmembers(module, isclass)]

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

                if isabstract(class_obj):
                    stored_modules[mod_name]["viewable"] = False
                else:
                    stored_modules[mod_name]["viewable"] = True

    return changed
