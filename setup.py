from setuptools import setup, find_packages

version = '0.0.1'

setup(name='ckanserviceprovider',
      version=version,
      description="A server that can server jobs at services.",
      long_description="""\
""",
      classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Open Knowledge Foundation',
      author_email='info@okfn.org',
      url='',
      license='AGPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
            'APScheduler==2.1.2',
            'Flask',
            'SQLAlchemy',
            'requests',
            'flask-admin',
            'flask-login'
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
