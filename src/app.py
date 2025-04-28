from flask import Flask, render_template, request, send_file, jsonify,session,redirect,url_for
import pandas as pd
from io import BytesIO
from main import main
from logger_config import logger
from datetime import timedelta
 
app = Flask(__name__)
app.secret_key = '4242'
app.permanent_session_lifetime = timedelta(minutes=5)
@app.route("/")
def index():
    return render_template("login.html")
 
@app.route("/filter_form",methods=["POST"])
def form():
     #app.logger.info("hi")sathya-inet branch
    username = request.form.get("user_name")
    password = request.form.get("password")
    print(password)
    if username == "admin" and password == "123" :
        session.permanent = True
        session["username"] = username   # store user info in session
        return jsonify({"message": "Login Successful", "redirect": "/index"}), 200
    else :
        return jsonify({"message": "Username or password incorrect!"}), 202
 
@app.route("/index")
def home():
    if "username" not in session:
        return redirect(url_for("index"))
    return render_template("index.html")
 
@app.route("/filter", methods=["POST"])
def filter_data():
    #app.logger.info("✅ filter_data() function is called!")
    try:
        # Get user inputs
        from_date = request.form.get("from_date")
        to_date = request.form.get("to_date")
        service_name = request.form.get("service_name")
        file = request.files.get("file")
        logger.info("------------------------------------------------------------------------------------")
        logger.info("Request received to filter data")
        print(f"Received: {from_date}, {to_date}, {service_name}, {file.filename if file else 'No file'}")
 
        # Validate inputs
        if not from_date or not to_date or not service_name or not file or file.filename == "":
            return "Missing required inputs!", 400
 
        # Process data using main()
        result = main(from_date, to_date, service_name, file)
       
        if not isinstance(result, dict):
            return "Invalid result format from main()!", 500
       
        if result["status"] == "202":  # Check if result is empty or explicitly 202
            return jsonify({"message": "No data found for the given date in uploaded excel!"}), 202  # Return JSON with a message
        else:
        # Convert result to an Excel file
            output_file = BytesIO()
            with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                for sheet_name in ["combined","mismatched", "not_in_Portal", "not_in_vendor", "VENDOR_SUCCESS_IHUB_INPROGRESS", "VENDOR_SUCCESS_IHUB_FAILED","not_in_Portal_vendor_success", "Vendor_failed_ihub_initiated"]:
                    if sheet_name in result:
                        result[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
 
            output_file.seek(0)
            logger.info(f"Report successfully exported for {service_name}")
            logger.info("----------------------------------------------------------------------------------")
            return send_file(output_file, download_name=f"{service_name}.xlsx", as_attachment=True)
        #return "Function Called", 200
    except Exception as e:
        logger.error(f"Error in filter_data(): {str(e)}")
        return f"Internal Server Error: {str(e)}", 500  # Return 500 with details
@app.route("/logout")
def logout():
    session.pop("username", None)
    return redirect(url_for("index"))
if __name__ == "__main__":
    app.run(debug=True)