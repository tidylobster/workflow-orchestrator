from setuptools import setup, find_packages

with open("version", 'r') as v:
    version = v.read().strip()

with open("README.md", "r") as file:
    readme = file.read()

setup(
    name='wo',
    version=version,
    description="Workflow Orchestration tool",
    long_description=readme,
    long_description_content_type='text/markdown',
    author="tidylobster",
    license="Apache 2.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "mlflow~=1.0",
        "boto3~=1.9.197",
        "google-cloud-storage~=1.16.1",
    ],
    setup_requires=[
        'pytest-runner'
    ],
    test_suite='tests',
    tests_require=[
        'pytest>=3.8.0',
    ],
)