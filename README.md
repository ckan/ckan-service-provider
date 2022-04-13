[![Tests](https://github.com/ckan/ckan-service-provider/actions/workflows/test.yml/badge.svg)](https://github.com/ckan/ckan-service-provider/actions/workflows/test.yml)
[![Latest Version](https://img.shields.io/pypi/v/ckanserviceprovider.svg)](https://pypi.python.org/pypi/ckanserviceprovider/)
[![Downloads](https://img.shields.io/pypi/dm/ckanserviceprovider.svg)](https://pypi.python.org/pypi/ckanserviceprovider/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/ckanserviceprovider.svg)](https://pypi.python.org/pypi/ckanserviceprovider/)
[![Development Status](https://img.shields.io/pypi/status/ckanserviceprovider.svg)](https://pypi.python.org/pypi/ckanserviceprovider/)
[![License](https://img.shields.io/pypi/l/ckanserviceprovider.svg)](https://pypi.python.org/pypi/ckanserviceprovider/)

[DataPusher]: https://github.com/okfn/datapusher
[PyPI]: https://pypi.python.org/pypi/ckanserviceprovider


# CKAN Service Provider

A library for making web services that make functions available as synchronous
or asynchronous jobs. Used by [DataPusher][].


## Getting Started

To install ckanserviceprovider for development:

```bash
git clone https://github.com/ckan/ckan-service-provider.git
cd ckan-service-provider
pip install -r requirements-dev.txt
```

To get started making a web service with ckanserviceprovider have a look at
[/example](example). You can run the example server with
`python example/main.py example/settings_local.py`.

For a real-world example have a look at [DataPusher][].


## Running the Tests

To run the ckanserviceprovider tests:

```bash
pytest
```


## Building the Documentation

To build the ckanserviceprovider docs:

```bash
python setup.py build_sphinx
```


## Releasing a New Version

To release a new version of ckanserviceprovider:

1. Increment the version number in [setup.py](setup.py)

2. Build a source distribution of the new version and publish it to
   [PyPI][]:

   ```bash
   python setup.py sdist bdist_wheel
   pip install --upgrade twine
   twine upload dist/*
   ```

   You may want to test installing and running the new version from PyPI in a
   clean virtualenv before continuing to the next step.

3. Commit your setup.py changes to git, tag the release, and push the changes
   and the tag to GitHub:

   ```bash
   git commit setup.py -m "Bump version number"
   git tag 0.0.1
   git push
   git push origin 0.0.1
   ```

   (Replace both instances of 0.0.1 with the number of the version you're
   releasing.)


## Authors

The original authors of ckanserviceprovider were
David Raznick <david.raznick@okfn.org> and
Dominik Moritz <dominik.moritz@okfn.org>. For the current list of contributors
see [github.com/ckan/ckan-service-provider/contributors](https://github.com/ckan/ckan-service-provider/contributors)
