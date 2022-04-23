from setuptools import setup

VERSION = "0.1.0"

setup(
       # the name must match the folder name 'verysimplemodule'
        name="spark-map", 
        version=VERSION,
        author="Pedro Duarte Faria",
        author_email="pedropark99@gmail.com",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=['pyspark'],
        keywords=['python', 'pyspark']
)