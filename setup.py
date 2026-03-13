#!/usr/bin/env python
import os
from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="millionsongs",
    version="0.0.1",
    author="Ferdinand van Wyk",
    description=(
        "An example of how to set up a Databricks project \
            with PySpark and pytest."
    ),
    license="GNU",
    keywords="databricks pytest tutorial pyspark",
    packages=["millionsongs", "tests"],
    long_description=read("README.md"),
    classifiers=[
        "Development Status :: 1 - Alpha",
    ],
)
