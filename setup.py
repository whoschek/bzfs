from setuptools import setup, find_packages

setup(
    name='wbackup_zfs',
    version='0.9.0',
    packages=find_packages(include=['wbackup_zfs', 'wbackup_zfs.*']),
    install_requires=[],
)
