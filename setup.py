from setuptools import setup

VERSION = "0.2.1"
DESCRIPTION = "Pyspark implementation of `map()` function for spark DataFrames"
LONG_DESCRIPTION = "README.md"


setup(
    name="spark_map", 
    version=VERSION,
    author="Pedro Duarte Faria",
    author_email="pedropark99@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    install_requires=['pyspark'],
    keywords=['python', 'pyspark']
)