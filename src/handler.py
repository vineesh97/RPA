from flask import jsonify


def handler(result):
    # Check if at least one value in result is a non-empty list (converted DataFrame)
    has_data = any(
        isinstance(value, list) and len(value) > 0 for value in result.values()
    )

    return jsonify({"isSuccess": has_data, "data": result})
