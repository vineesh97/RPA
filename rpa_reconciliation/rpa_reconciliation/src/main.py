from reconciliation import run_reconciliation

if __name__ == "__main__":
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")
    result = run_reconciliation(start_date, end_date)
    
    print("Not in Excel:", result["not_in_excel"])
    print("Not in Server:", result["not_in_server"])
    print("Mismatched:", result["mismatched"])
