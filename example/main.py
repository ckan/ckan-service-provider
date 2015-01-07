import os

import ckanserviceprovider.web as web

import example.jobs as jobs


def _create_app(config_file_name):
    """Return a CKAN Service Provider app with the example jobs registered."""
    os.environ['JOB_CONFIG'] = config_file_name
    app = web.CKANServiceProvider(__name__)
    app.register_synchronous_jobs(jobs.example_echo)
    app.register_asynchronous_jobs(
        jobs.example_async_echo, jobs.example_async_ping)
    return app


def serve(config_file_name):
    """Run the example app in a local development web server."""
    _create_app(config_file_name).run()


def test_app(config_file_name):
    """Return a Flask test client for the example app."""
    return _create_app(config_file_name).test_client()


def main():
    """Parse the given command line args and run the app in a dev server."""
    import argparse

    argparser = argparse.ArgumentParser(
        description='Example service',
        epilog='''"For a moment, nothing happened.
            Then, after a second or so, nothing continued to happen."''')

    argparser.add_argument('config', metavar='CONFIG', type=file,
                           help='configuration file')
    args = argparser.parse_args()

    serve(os.path.abspath(args.config.name))


if __name__ == '__main__':
    main()
