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
        'config',
        'pandas',
        'pytest',
        'elasticsearch',
        'pydantic',
        'tenacity',
        'zmq',
        'pyzmq',
        'kafka-python',
        'cachetools',
        'numpy',
        'nltk',
        'sqlalchemy',
        'setuptools',
        'ujson',
        'dacite',
        'onnxruntime',
        'psycopg2-binary'
    ],

)
