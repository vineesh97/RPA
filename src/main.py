from reconciliation import run_Reconciliation
import pandas as pd
from logger_config import logger
import sys  

def main(from_date, to_date, service_name, file):
    try:
        # Read the uploaded Excel file
        df_excel = pd.read_excel(file, dtype=str)
        #print(df_excel)
        # Convert the DATE column to datetime
        df_excel["DATE"] = pd.to_datetime(df_excel["DATE"], errors="coerce").dt.date

        # Convert user input dates to datetime
        from_date = pd.to_datetime(from_date).date()
        to_date = pd.to_datetime(to_date).date()

        # Filter records within the given date range
        Date_check = df_excel[(df_excel["DATE"] >= from_date) & (df_excel["DATE"] <= to_date)]

        if Date_check.empty:
            logger.warning("No records found within the given date range!")
            return {"status":"202"}
        else:
            logger.info("Records found within the date range. Running reconciliation...")

            # Call the reconciliation function
            result = run_Reconciliation(from_date, to_date, service_name, df_excel)
            logger.info("Reconciliation Ends")
            return result  # Should be a dictionary of DataFrames

    except Exception as e:
        logger.error(f"❌ Error in main(): {str(e)}")
        return {"error": f"Internal Server Error: {str(e)}"}
