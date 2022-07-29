import setuptools
from setuptools import setup

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
        "logsight.analytics_core.modules.anomaly_detection.models": ["*.pickle", "*.onnx", "*.json"]
    },
    install_requires=[
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
