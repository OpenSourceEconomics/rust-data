#!/usr/bin/env python
"""Run project.

This script simply runs all files in the repository
to ensure all is working properly throughout.

"""
import os
import subprocess as sp

PROJECT_ROOT = os.environ["PROJECT_DIR"]


def run_notebook(notebook):
    cmd = f" jupyter nbconvert --execute {notebook} --ExecutePreprocessor.timeout=-1 --to html"
    sp.check_call(cmd, shell=True)


# We want to make sure all notebooks run.
os.chdir(PROJECT_ROOT)
os.chdir("docs")
run_notebook("replicate_descriptives.ipynb")
os.chdir(PROJECT_ROOT)
