from reconciliation import run_reconciliation

if __name__ == "__main__":
    #Initial execution Getting from and to date from user
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")
   
    #calling the function module
    result = run_reconciliation(start_date, end_date)

    #Printing values for our reference
    #print("Not in Excel:", result["not_in_excel"])
    #print("Not in Server:", result["not_in_server"])
    #print("Mismatched:", result["mismatched"])

    #Exporting Seperated data to excel
    result["mismatched"].to_excel("D:\\GitHub\\RPA\\rpa_reconciliation\\rpa_reconciliation\\data\\Mismatched_values.xlsx",index=False)
    result["not_in_server"].to_excel("D:\\GitHub\\RPA\\rpa_reconciliation\\rpa_reconciliation\\data\\NotinHub_values.xlsx",index=False)
    result["not_in_excel"].to_excel("D:\\GitHub\\RPA\\rpa_reconciliation\\rpa_reconciliation\\data\\NotinVendor_values.xlsx",index=False)
