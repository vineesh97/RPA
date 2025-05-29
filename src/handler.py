from flask import jsonify
from logger_config import logger


def handler(result, message, service_name):
    # Check if at least one value in result is a non-empty list (converted DataFrame)
    # print(type(result))
    if isinstance(result, str):
        logger.info("Error result sent as API")
        logger.info("----------------------------------")
        return jsonify(
            {
                "isSuccess": False,
                "data": result,
                "message": message,
                "service_name": service_name,
            }
        )
    else:
        has_data = any(
            isinstance(value, list) and len(value) > 0 for value in result.values()
        )
        logger.info("Result sent as API")
        logger.info("----------------------------------")

        return jsonify(
            {
                "isSuccess": has_data,
                "data": result,
                "message": "",
                "service_name": service_name,
            }
        )
