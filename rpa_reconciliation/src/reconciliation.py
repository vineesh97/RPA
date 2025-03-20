import pandas as pd
from db_connector import get_db_connection
from config import CONFIG

def run_reconciliation(start_date, end_date, service_name):
    engine = get_db_connection()

    query = f'''
        select mt2.TransactionRefNum ,prt.requestID as vendor_reference ,mt.TransactionStatus  as Tenant_Status,mt2.TransactionStatus  as HUB_Master_status, 
        mst.TransactionStatus as MasterSubTrans_status,prt.rechargeStatus  as  Recharge_status
        from iHubTenantPortal_dev.MasterTransaction mt 
        left join ihub_dev.MasterTransaction mt2 
        on mt.Id =mt2.TenantMasterTransactionId 
        left join ihub_dev.MasterSubTransaction mst 
        on mst.MasterTransactionId  =mt2.Id
        left join ihub_dev.{service_name} prt 
        on prt.MasterSubTransactionId =mst.Id 
        where mt2.TenantDetailId = 1 and
        DATE(prt.CreationTs) BETWEEN '{start_date}' AND '{end_date}' '''
    
   #Reading data from both Server and Excel mkm
    df_db = pd.read_sql(query, con=engine)
    df_excel = pd.read_excel(CONFIG["excel_file"], dtype=str)

    #replacing the enums to its corresponding status values
    df_db["Recharge_status"]=df_db["Recharge_status"].apply(lambda x: "success" if x == 1 else "pending" if x == 2 else "failed" if x == 3 else "instant failed" if x==4 else x )
    columns_to_update = ["HUB_Master_status", "MasterSubTrans_status","Tenant_Status"]
    df_db[columns_to_update] = df_db[columns_to_update].apply(lambda x: x.map({0:"inititated",1: "success", 2: "failed", 3: "In progress",4:"Partial success"}).fillna(x))

    #df_db.to_excel("D:\\GitHub\\RPA\\rpa_reconciliation\\rpa_reconciliation\\data\\Test.xlsx",index=False)
    #Seperating data not present in vendor_statement but present in I HUB Database 
    not_in_excel = df_db[~df_db["vendor_reference"].isin(df_excel["REFID"])][["vendor_reference", "Recharge_status"]]
    
    #Seperating data present in vendor_statement but NOT present in I HUB Database 
    not_in_server = df_excel[~df_excel["REFID"].isin(df_db["vendor_reference"])][["REFID", "USERNAME", "AMOUNT", "STATUS", "DATE"]]
    
    #Seperating data  matching in vendor_statement and  in I HUB Database 
    matched = df_db.merge(df_excel, left_on="vendor_reference", right_on="REFID", how="inner")
    
    #Seperating data matching in both but having diff status value 
    mismatched = matched[matched["Recharge_status"].str.lower() != matched["STATUS"].str.lower()]
   # return 0
   #VENDOR_SUCCESS_IHUB_INPROGRESS
    VENDOR_SUCCESS_IHUB_INPROGRESSS = mismatched[
            (mismatched['STATUS'] == 'Success') &  # STATUS in df_recharge_excel is Success
            (mismatched['Recharge_status'] == "success") &  # Recharge_status in df1 is Success
            (mismatched['HUB_Master_status'] == "In progress" )  # Mst_status in df1 is NOT Success
    ]
   #VENDOR_SUCCESS_IHUB_FAILED
    VENDOR_SUCCESS_IHUB_FAILED = mismatched[
            (mismatched['STATUS'] == 'Success') &  # STATUS in df_recharge_excel is Success
            (mismatched['Recharge_status'] == "success") &  # Recharge_status in df1 is Success
            (mismatched['HUB_Master_status'] == "failed" )  # Mst_status in df1 is NOT Success
    ]


   
    return {"not_in_excel": not_in_excel, "not_in_server": not_in_server.head(100), "mismatched": mismatched,"VENDOR_SUCCESS_IHUB_INPROGRESS":VENDOR_SUCCESS_IHUB_INPROGRESS ,"VENDOR_SUCCESS_IHUB_FAILED":VENDOR_SUCCESS_IHUB_FAILED}  




