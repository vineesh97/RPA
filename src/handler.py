from flask import jsonify


def handler(result, message):
    # Check if at least one value in result is a non-empty list (converted DataFrame)
    # print(type(result))
    if isinstance(result, str):
        return jsonify({"isSuccess": False, "data": result, "message": message})
    else:
        has_data = any(
            isinstance(value, list) and len(value) > 0 for value in result.values()
        )

        return jsonify({"isSuccess": has_data, "data": result, "message": ""})
