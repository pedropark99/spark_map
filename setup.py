from setuptools import setup
import toml

# Import package metadata from the pyproject.toml file.
# This way we mantain these metadata in only one file, and just reuse as needed.
metadata = toml.load("pyproject.toml")
project = metadata['project']


setup(
    name = project['name'],
    version = project['version'],
    author = project['authors'][0]['name'],
    author_email = project['authors'][0]['email'],
    license = 'MIT License',
    description = project['description'],
    long_description = project['readme'],
    classifiers = project['classifiers'],
    url = project['urls']['Homepage'],
    keywords = ['spark', 'pyspark', 'map'],
    install_requires = project['dependencies']
)