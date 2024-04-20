from setuptools import setup, Extension
from Cython.Build import cythonize

ext_modules = [
        Extension("job", 
                  sources=["job.pyx"], 
                  include_dirs=["./lib"],
                  language="c++"),
        Extension("utils", 
                  sources=["utils.pyx"], 
                  include_dirs=["./lib"],
                  language="c++"),
]

setup(ext_modules=cythonize(ext_modules))
