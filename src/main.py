from reconciliation import run_reconciliation
import pandas as pd

if __name__ == "__main__":
    #Initial execution Getting from and to date from user
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")
    service_name = input("enter service name: ")
   
    #calling the function module
    result = run_reconciliation(start_date, end_date,service_name)

    #Printing values for our reference
    #print("Not in Excel:", result["not_in_excel"])
    #print("Not in Server:", result["not_in_server"])
    #print("Mismatched:", result["mismatched"])

    #Exporting Seperated data to excel
    #result["mismatched"].to_excel("D:\\Github\\RPA\\data\\Mismatched_values.xlsx",index=False)
    #result["not_in_server"].to_excel("D:\\GitHub\\RPA\\rpa_reconciliation\\rpa_reconciliation\\data\\NotinHub_values.xlsx",index=False)
    #result["not_in_excel"].to_excel("D:\\GitHub\\RPA\\rpa_reconciliation\\rpa_reconciliation\\data\\NotinVendor_values.xlsx",index=False)
output_file = f"D:\\Github\\RPA\\data\\{service_name}.xlsx"

# Use ExcelWriter to write multiple sheets in the same Excel file
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        result["mismatched"].to_excel(writer, sheet_name="Mismatched Data", index=False)
        result["not_in_Portal"].to_excel(writer, sheet_name="not_in_Portal", index=False)
        result["not_in_vendor"].to_excel(writer, sheet_name="ot_in_vendor", index=False)
        result["VENDOR_SUCCESS_IHUB_INPROGRESS"].to_excel(writer, sheet_name="Vendor Success IHUB InProgress", index=False)
        result["VENDOR_SUCCESS_IHUB_FAILED"].to_excel(writer, sheet_name="Vendor Success IHUB Failed", index=False)

print(f"Report successfully saved to {output_file}")