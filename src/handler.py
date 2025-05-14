from flask import jsonify


def handler(result):
    if result["status"] == "200":
        is_success = {"isSuccess": True, "data": result}
        return jsonify(is_success)
    else:
        is_failure = {"isSuccess": False, "data": result}
        return jsonify(is_failure)
