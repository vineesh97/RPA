from reconciliation import run_Reconciliation
import pandas as pd
from logger_config import logger
from config import CONFIG
import sys  

if __name__ == "__main__":
    #Initial execution Getting from and to date from user
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")
    service_name = input("enter service name: ")

    df_excel = pd.read_excel(f"D:\\Github\\RPA\\data\\uploading_excel\\{service_name}.xlsx", dtype=str)
    print(df_excel)
    df_excel["DATE"] = pd.to_datetime(df_excel["DATE"], errors="coerce").dt.date
    from_date = pd.to_datetime(f"{start_date}").date()  # Change as needed
    to_date = pd.to_datetime(f"{end_date}").date() 

    Date_check = df_excel[(df_excel["DATE"] >= from_date) & (df_excel["DATE"] <= to_date)]

    if Date_check.empty:
        print("⚠️ No records found within the given date range!.Kindly the Date Entered")
        sys.exit(0)
    else:
        print("✅ Records found within the date range:")
    #calling the function module
        result = run_Reconciliation(start_date, end_date,service_name,df_excel)

output_file = f"D:\\Github\\RPA\\data\\exported_excel\\{service_name}.xlsx"

# Use ExcelWriter to write multiple sheets in the same Excel file
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        result["mismatched"].to_excel(writer, sheet_name="Mismatched Data", index=False)
        result["not_in_Portal"].to_excel(writer, sheet_name="not_in_Portal", index=False)
        result["not_in_vendor"].to_excel(writer, sheet_name="not_in_vendor", index=False)
        result["VENDOR_SUCCESS_IHUB_INPROGRESS"].to_excel(writer, sheet_name="Vendor Success IHUB InProgress", index=False)
        result["VENDOR_SUCCESS_IHUB_FAILED"].to_excel(writer, sheet_name="Vendor Success IHUB Failed", index=False)

print(f"Report successfully saved to {output_file}")
logger.info("Report successfully Exported")
logger.info("-----------------------------------------------------------------------------------------------------")
