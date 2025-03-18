import pandas as pd
from src.db_connector import get_db_connection
from src.config import CONFIG

def run_reconciliation(start_date, end_date):
    engine = get_db_connection()

    query = f'''
        SELECT prt.requestID AS vendor_reference, prt.rechargeStatus AS status,
               DATE(prt.CreationTs) AS date,
               CASE
                   WHEN prt.rechargeStatus = 0 THEN 'Initiated'
                   WHEN prt.rechargeStatus = 1 THEN 'Success'
                   WHEN prt.rechargeStatus = 2 THEN 'Failed'
                   WHEN prt.rechargeStatus = 3 THEN 'In Progress'
                   WHEN prt.rechargeStatus = 4 THEN 'PartialSuccess'
               END AS status_name
        FROM ihub_dev.PsRechargeTransaction prt
        WHERE DATE(prt.CreationTs) BETWEEN '{start_date}' AND '{end_date}'
    '''
    
    df_db = pd.read_sql(query, con=engine)
    df_excel = pd.read_excel(CONFIG["excel_file"], dtype=str)
    
    not_in_excel = df_db[~df_db["vendor_reference"].isin(df_excel["REFID"])][["vendor_reference", "status_name"]]
    not_in_server = df_excel[~df_excel["REFID"].isin(df_db["vendor_reference"])][["REFID", "USERNAME", "AMOUNT", "STATUS", "DATE"]]
    
    matched = df_db.merge(df_excel, left_on="vendor_reference", right_on="REFID", how="inner")
    mismatched = matched[matched["status_name"].str.lower() != matched["STATUS"].str.lower()]
    
    return {"not_in_excel": not_in_excel, "not_in_server": not_in_server, "mismatched": mismatched}
