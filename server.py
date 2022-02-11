"""
Entry point
Spawns a server that coodinates the operations
"""
import argparse
from flask import Flask, request, jsonify

parser = argparse.ArgumentParser(description='Starts the server to process tracks')
parser.add_argument('-p', '--port', dest='port', metavar='p', type=int,
        default=5000,
        help='port to use')
parser.add_argument('-H', '--host', dest='host', metavar='h', type=str,
        default='0.0.0.0',
        help='host name')
parser.add_argument('--debug', dest='debug', action='store_true',
        default=False,
        help='print server debug information')
parser.add_argument('--verbose', dest='verbose',
        action='store_false',
        help='print debug information of processing stage')
parser.add_argument('--config', '-c', dest='config', metavar='c', type=str,
        help='configuration file')
args = parser.parse_args()

app = Flask(__name__)

def set_headers(response):
    """ Sets appropriate headers

    Args:
        response (:obj:`flask.response`)
    Returns:
        :obj:`flask.response`
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/test', methods=['GET'])
def test():
    """
    test route

    Returns:
        :string
    """
    response = jsonify({"marco": "polo"})
    return set_headers(response)

if __name__ == '__main__':
    app.run(debug=args.debug, port=args.port, host=args.host, threaded=True)
