from setuptools import setup, find_packages

setup(
    name='etsin_finder_search',
    description='Etsin finder search index related scripts',
    author='CSC - IT Center for Science Ltd.',
    packages=find_packages(exclude=['scripts']),
    setup_requires=[
        'pytest-runner'
    ]
)
