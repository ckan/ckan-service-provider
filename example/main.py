import os

import ckanserviceprovider.web as web

import jobs

# check whether jobs have been imported properly
assert jobs.echo


def serve():
    web.init()
    web.app.run(web.app.config.get("HOST"), web.app.config.get("PORT"))


def serve_test():
    web.init()
    return web.app.test_client()


def main():
    import argparse

    argparser = argparse.ArgumentParser(
        description="Example service",
        epilog='''"For a moment, nothing happened.
            Then, after a second or so, nothing continued to happen."''',
    )

    argparser.add_argument(
        "config",
        metavar="CONFIG",
        type=argparse.FileType("r"),
        help="configuration file",
    )
    args = argparser.parse_args()

    os.environ["JOB_CONFIG"] = os.path.abspath(args.config.name)
    serve()


if __name__ == "__main__":
    main()
