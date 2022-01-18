# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from os import path
# io.open is needed for projects that support Python 2.7
# It ensures open() defaults to text mode with universal newlines,
# and accepts an argument to specify the text encoding
# Python 3 only projects can skip this import
# https://github.com/pypa/sampleproject/blob/master/setup.py
from io import open

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()
    
setup(
    name='lambda_actor',
    version='0.5.6',
    description='Actor model for aws lambda',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='rv',
    author_email='yo-maruya@rescala.jp',
    keywords='lambda actor model',
    install_requires=['dataclasses_json'],
    url='https://www.rescala.jp',
    license='MIT',
    packages=find_packages(exclude=('tests')),
    python_requires='>=3.7',
)
