from flask import (
    Flask,
    render_template,
    request,
    send_file,
    jsonify,
    session,
    redirect,
    url_for,
    make_response,
)
import pandas as pd
from io import BytesIO
from main import main
from logger_config import logger
from datetime import timedelta
from handler import handler
from flask_cors import CORS
import traceback
from fastapi.responses import JSONResponse


app = Flask(__name__)
app.secret_key = "4242"
CORS(
    app, supports_credentials=True, origins=["http://localhost:3000"]
)  # Adjust origin as needed
app.permanent_session_lifetime = timedelta(minutes=30)


# Error handler for 404
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Resource not found"}), 404


# Error handler for 500
@app.errorhandler(500)
def internal_error(e):
    logger.error(f"500 Error: {str(e)}\n{traceback.format_exc()}")
    return jsonify({"error": "Internal server error"}), 500


@app.route("/")
def land():
    return render_template("login.html")


# Test endpoint
@app.route("/api/test", methods=["GET"])
def test_endpoint():
    """Test endpoint to verify API is working"""
    return jsonify(
        {
            "status": "success",
            "message": "API is working!",
            "data": {"service": "test", "version": "1.0"},
        }
    )


# Login endpoint for Next.js
@app.route("/api/login", methods=["POST"])
def login_form():
    try:
        if not request.is_json:
            return jsonify({"error": "Request must be JSON"}), 400

        data = request.get_json()
        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return jsonify({"error": "Username and password are required"}), 400

        # In production, use proper password hashing and database lookup
        if username == "admin" and password == "123":
            session.permanent = True
            session["username"] = username
            return (
                jsonify({"message": "Login Successful", "redirect": "/filter_form"}),
                200,
            )
        else:
            return jsonify({"error": "Invalid credentials"}), 401
    except Exception as e:
        logger.error(f"Login error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"error": "Login failed"}), 500


@app.route("/login")
def login_page():
    return render_template("login.html")


@app.route("/index", methods=["POST"])
def form():
    try:
        username = request.form.get("user_name")
        password = request.form.get("password")

        if not username or not password:
            return jsonify({"error": "Username and password are required"}), 400

        if username == "admin" and password == "123":
            session.permanent = True
            session["username"] = username
            return (
                jsonify({"message": "Login Successful", "redirect": "/filter_form"}),
                200,
            )
        else:
            return jsonify({"error": "Invalid credentials"}), 401
    except Exception as e:
        logger.error(f"Form login error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"error": "Login failed"}), 500


@app.route("/filter_form")
def home():
    if "username" not in session:
        return redirect(url_for("login_page"))

    response = make_response(render_template("index.html"))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/api/dummydata", methods=["POST"])
def dummydata():
    try:
        # # Validate session
        # if "username" not in session:
        #     return jsonify({"error": "Unauthorized"}), 401

        # Validate required fields
        required_fields = ["from_date", "to_date", "service_name"]
        for field in required_fields:
            if field not in request.form:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Get form data
        from_date = request.form.get("from_date")
        to_date = request.form.get("to_date")
        service_name = request.form.get("service_name")
        transaction_type = request.form.get("transaction_type", None)
        file = request.files.get("file")

        # Validate file
        if not file or file.filename == "":
            return jsonify({"error": "No file uploaded"}), 400

        # Process data
        result = main(from_date, to_date, service_name, file, transaction_type)

        for key, value in result.items():
            # Convert pandas DataFrames to dict
            if isinstance(value, pd.DataFrame):
                # Convert using pandas' built-in NaN handling
                value = value.where(pd.notnull(value), None)
                for col in value.select_dtypes(include=["datetime64[ns]"]).columns:
                    value[col] = (
                        value[col].astype(object).where(pd.notnull(value[col]), None)
                    )
                result[key] = value.to_dict(orient="records")

            elif isinstance(value, list):
                # Ensure any lists contain proper serializable objects
                result[key] = [
                    dict(item) if hasattr(item, "__dict__") else item for item in value
                ]

            elif hasattr(value, "__dict__"):
                # Convert objects to dictionaries
                result[key] = value.__dict__

        return handler(result)
    except Exception as e:
        logger.error(f"Dummydata error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to process data"}), 500


@app.route("/filter", methods=["POST"])
def filter_data():
    try:
        # Validate session
        if "username" not in session:
            return jsonify({"error": "Unauthorized"}), 401

        # Get form data
        from_date = request.form.get("from_date")
        to_date = request.form.get("to_date")
        service_name = request.form.get("service_name")
        transaction_type = request.form.get("transaction_type")
        file = request.files.get("file")

        # Validate inputs
        if not all([from_date, to_date, service_name, transaction_type]) or not file:
            return jsonify({"error": "Missing required inputs"}), 400

        # Process data
        result = main(from_date, to_date, service_name, file, transaction_type)

        if not isinstance(result, dict):
            return jsonify({"error": "Invalid result format"}), 500

        if result.get("status") == "202":
            return (
                jsonify(
                    {
                        "message": "No data found for the given date range",
                        "data": result,
                    }
                ),
                202,
            )

        # Create Excel file
        output_file = BytesIO()
        with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
            sheets = [
                "combined",
                "mismatched",
                "not_in_Portal",
                "not_in_vendor",
                "VENDOR_SUCCESS_IHUB_INPROGRESS",
                "VENDOR_SUCCESS_IHUB_FAILED",
                "not_in_Portal_vendor_success",
                "Vendor_failed_ihub_initiated",
                "Tenant_db_ini_not_in_hubdb",
            ]

            for sheet_name in sheets:
                if sheet_name in result and not result[sheet_name].empty:
                    result[sheet_name].to_excel(
                        writer, sheet_name=sheet_name, index=False
                    )

        output_file.seek(0)
        logger.info(f"Report generated for {service_name}")

        return send_file(
            output_file,
            download_name=f"{service_name}_report.xlsx",
            as_attachment=True,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        logger.error(f"Filter data error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to generate report"}), 500


@app.route("/logout")
def logout():
    session.clear()
    return jsonify({"message": "Logged out successfully"}), 200


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
