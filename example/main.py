import os

import ckanserviceprovider.web as web

import jobs

# check whether jobs have been imported properly
assert(jobs.example_echo)


def serve():
    web.init()
    web.app.run(web.app.config.get('HOST'), web.app.config.get('PORT'))


def test_app():
    """Return a Flask test client for the example CKAN Service Provider app.

    You should call web.init() once before calling this, then you can call
    this function as many times as you want to get test apps.

    """
    return web.app.test_client()


def main():
    import argparse

    argparser = argparse.ArgumentParser(
        description='Example service',
        epilog='''"For a moment, nothing happened.
            Then, after a second or so, nothing continued to happen."''')

    argparser.add_argument('config', metavar='CONFIG', type=file,
                           help='configuration file')
    args = argparser.parse_args()

    os.environ['JOB_CONFIG'] = os.path.abspath(args.config.name)
    serve()

if __name__ == '__main__':
    main()
