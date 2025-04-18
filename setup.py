from setuptools import setup, find_packages


setup(
    name='aussiebb-carbon',
    version='0.0.1-dev',
    description="Provides the 'Carbon' Python class to simplify access to the Aussie Broadband Carbon API",
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        'requests',
        'pandas',
        'netaddr',
    ],
)
