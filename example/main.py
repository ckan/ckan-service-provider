import os

import ckanserviceprovider.web as web

import jobs

# check whether jobs have been imported properly
assert(jobs.echo)

# for gunicorn
app = web.app


def serve():
    web.configure()
    web.run()


def serve_test():
    web.configure()
    return web.test_client()


def main():
    import argparse

    argparser = argparse.ArgumentParser(
        description='Example service',
        epilog='''"For a moment, nothing happened.
            Then, after a second or so, nothing continued to happen."''')

    argparser.add_argument('config', metavar='CONFIG', type=file,
                           help='configuration file')
    args = argparser.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(os.path.dirname(here), args.config.name)

    os.environ['JOB_CONFIG'] = config_path
    serve()

if __name__ == '__main__':
    main()
