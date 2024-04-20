from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy

ext_modules = [
        Extension("abstractgen", 
                  sources=["abstractgen.pyx"],
                  include_dirs=[numpy.get_include()],
                  language="c++"),
        Extension("randomgen", 
                  sources=["randomgen.pyx"],
                  language="c++"),
        Extension("dictgen", 
                  sources=["dictgen.pyx"],
                  language="c++"),
        Extension("listgen", 
                  sources=["listgen.pyx"],
                  language="c++"),
        ]

setup(ext_modules=cythonize(ext_modules, include_path=["../jobs"]))
