#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="target-cinch",
    version="0.0.6",
    description="Singer.io target for loading data into Cinch",
    author="Cinch",
    url="https://github.com/cinchio/target-cinch",
    python_requires='>=3.6.0',
    py_modules=["target_cinch"],
    install_requires=[
        "singer-python==5.12.1",
        "requests==2.26.0",
    ],
    entry_points="""
    [console_scripts]
    target-cinch=target_cinch:main
    """,
    packages=find_packages(include=['target_cinch', 'target_cinch.*']),
    #package_data = {
    #    "target_cinch": ["schemas/*.json"]
    #},
    #include_package_data=True,
)
