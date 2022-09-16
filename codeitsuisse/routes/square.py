import logging
import json
import challenge.entery_challenge as ec

from flask import request, jsonify

from codeitsuisse import app

logger = logging.getLogger(__name__)

@app.route('/square1', methods=['POST'], endpoint='evaluate1')
def evaluate():
    data = request.get_json()
    logging.info("data sent for evaluation {}".format(data))
    inputValue = data.get("input")
    result = inputValue * inputValue
    logging.info("My result :{}".format(result))
    return json.dumps(result)

@app.route('/tickerStreamPart1', methods=['POST'], endpoint='evaluate3')
def evaluate():
    data = request.get_json()
    logging.info("data sent for evaluation {}".format(data))
    inputValue = data.get("stream")
    result = { 
        "output":  ec.to_cumulative(inputValue)
    }
    logging.info("My result :{}".format(result))
    return json.dumps(result)

@app.route('/tickerStreamPart2', methods=['POST'], endpoint='evaluate4')
def evaluate():
    data = request.get_json()
    logging.info("data sent for evaluation {}".format(data))
    inputValue = data.get("stream")
    inputValueqq = data.get("quantityBlock")
    result = { 
        "output":  ec.to_cumulative_delayed(inputValue, inputValueqq)
    }
    logging.info("My result :{}".format(result))
    return json.dumps(result)

