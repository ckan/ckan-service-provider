from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='ckan-service-prototype',
      version=version,
      description="An example ckan service",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='David Raznick',
      author_email='kindly@gmail.com',
      url='',
      license='AGPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=['''
            APScheduler
            Flask
            SQLAlchemy
            requests'''
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
