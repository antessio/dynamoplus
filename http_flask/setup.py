from setuptools import setup, find_packages

setup(
    name='dynamoplus-http-server',
    version='0.5.0',
    description='HTTP API to access to dynamoplus',
    author='antessio',
    author_email='antessio7@gmail.com',
    url='https://github.com/antessio/dynamoplus',
    packages=find_packages(),
    install_requires=[
        # List the dependencies of your core library here
        # For example: 'requests', 'numpy', etc.
    ],
    classifiers=[],
)
