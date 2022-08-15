import os
import subprocess
import sys

import setuptools
from setuptools import setup

try:
    import git
except ModuleNotFoundError:
    subprocess.call([sys.executable, '-m', 'pip', 'install', 'gitpython'])
    import git


def pull_first():
    """This script is in a git directory that can be pulled."""
    cwd = os.getcwd()
    gitdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(gitdir)
    g = git.cmd.Git(gitdir)
    try:
        g.execute(['git', 'lfs', 'install'])
        g.execute(['git', 'lfs', 'pull'])
    except git.exc.GitCommandError:
        raise RuntimeError("Make sure git-lfs is installed!")
    os.chdir(cwd)


pull_first()

setup(
    name='logsight',
    packages=setuptools.find_packages(),
    description='Logsight core - internal library',
    version='1.3.0',
    author='Logsight.ai',
    author_email='contact@logsight.ai',
    keywords=['analytics', 'logsight', 'log'],
    url="https://github.com/aiops/logsight",
    python_requires=">=3.8",
    include_package_data=True,
    package_data={
        "logsight.logger": ['*.cfg'],
        "logsight.configs": ['*.cfg'],
        "logsight.analytics_core.modules.anomaly_detection.models": ["*.pickle", "*.onnx", "*.json"],
        "logsight.analytics_core.modules.log_parsing.parsing_lib": ["*.ini"]
    },
    install_requires=[
        "pandas~=1.4.3",
        "config~=0.5.1",
        "pytest~=7.1.2",
        "elasticsearch==8.3.3",
        "pydantic==1.9.2",
        "tenacity~=8.0.1",
        "zmq~=0.0.0",
        "pyzmq~=22.3.0",
        "cachetools~=5.2.0",
        "numpy~=1.23.2",
        "nltk~=3.7",
        "sqlalchemy~=1.4.40",
        "setuptools~=65.0.0",
        "ujson~=5.4.0",
        "kafka-python~=2.0.2",
        "dacite~=1.6.0",
        "onnxruntime~=1.12.1",
        "psycopg2-binary~=2.9.3",
        "gitpython~=3.1.27",
        "pytest-cov~=3.0.0"
    ],

)
