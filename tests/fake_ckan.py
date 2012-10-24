from flask import Flask, request, json, jsonify
import os

app = Flask(__name__,
            static_folder=os.path.join(os.path.dirname(
                os.path.realpath(__file__)), "static"))

request_store = []


def store_request():
    try:
        data = request.json
        headers = request.headers
        request_store.append(
            {"data": data,
             "headers": dict(request.headers)}
        )
        return 'ok'
    except Exception, e:
        request_store.append(str(e))
        raise


@app.route("/result", methods=['GET', 'POST'])
def result():
    return store_request()


@app.route("/last_request", methods=['GET', 'POST'])
def last_request():
    return jsonify(request_store.pop())


@app.route("/", methods=['GET', 'POST'])
def ok():
    return 'ok'

if __name__ == "__main__":
    app.run(port=9091)
