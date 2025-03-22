import pandas as pd
from db_connector import get_db_connection
from config import CONFIG
from logger_config import logger
engine = get_db_connection()

def run_reconciliation(start_date,end_date,service_name):
    logger.info("---------Entering Reconciliation---------")

    start_date=start_date
    end_date=end_date
    if service_name=='recharge':
        result = recharge(start_date,end_date)
    return result

def filtering_data (df_db,df_excel):
    logger.info("---Filteration Starts:")

    columns_to_update = ["HUB_Master_status", "MasterSubTrans_status","Tenant_Status"]
    df_db[columns_to_update] = df_db[columns_to_update].apply(lambda x: x.map({0:"inititated",1: "success", 2: "failed", 3: "In progress",4:"Partial success"}).fillna(x))

    #df_db.to_excel("D:\\GitHub\\RPA\\rpa_reconciliation\\rpa_reconciliation\\data\\Test.xlsx",index=False)
    #Seperating data not present in vendor_statement but present in I HUB Database 
    not_in_vendor= df_db[~df_db["vendor_reference"].isin(df_excel["REFID"])][["vendor_reference", "Recharge_status"]]
    
    #Seperating data present in vendor_statement but NOT present in I HUB Database 
    not_in_Portal= df_excel[~df_excel["REFID"].isin(df_db["vendor_reference"])][["REFID", "USERNAME", "AMOUNT", "STATUS", "DATE"]]
    
    #Seperating data  matching in vendor_statement and  in I HUB Database 
    matched = df_db.merge(df_excel, left_on="vendor_reference", right_on="REFID", how="inner")
    
    #Seperating data matching in both but having diff status value 
    mismatched = matched[matched["Recharge_status"].str.lower() != matched["STATUS"].str.lower()]
   # return 0
   #VENDOR_SUCCESS_IHUB_INPROGRESS
    VENDOR_SUCCESS_IHUB_INPROGRESS = mismatched[
            (mismatched['STATUS'] == 'Success') &  # STATUS in df_recharge_excel is Success
            (mismatched['Recharge_status'] == "success") &  # Recharge_status in df1 is Success
            (mismatched['HUB_Master_status'] == "In progress" )  # Mst_status in df1 is NOT Success
    ]
   #VENDOR_SUCCESS_IHUB_FAILED___
    VENDOR_SUCCESS_IHUB_FAILED = mismatched[
            (mismatched['STATUS'] == 'Success') &  # STATUS in df_recharge_excel is Success
            (mismatched['Recharge_status'] == "success") &  # Recharge_status in df1 is Success
            (mismatched['HUB_Master_status'] == "failed" )  # Mst_status in df1 is NOT Success
    ]
    logger.info("---Filteration Ends:")
    return {"not_in_vendor": not_in_vendor, "not_in_Portal": not_in_Portal.head(100), "mismatched": mismatched,"VENDOR_SUCCESS_IHUB_INPROGRESS":VENDOR_SUCCESS_IHUB_INPROGRESS ,"VENDOR_SUCCESS_IHUB_FAILED":VENDOR_SUCCESS_IHUB_FAILED}  

def recharge(start_date, end_date):
    logger.info("--Service mapping---")
    query = f'''
        select mt2.TransactionRefNum ,sn.requestID as vendor_reference ,mt.TransactionStatus  as Tenant_Status,mt2.TransactionStatus  as HUB_Master_status, 
        mst.TransactionStatus as MasterSubTrans_status,sn.rechargeStatus  as  Recharge_status
        from iHubTenantPortal_dev.MasterTransaction mt 
        left join ihub_dev.MasterTransaction mt2 
        on mt.Id =mt2.TenantMasterTransactionId 
        left join ihub_dev.MasterSubTransaction mst 
        on mst.MasterTransactionId  =mt2.Id
        left join ihub_dev.PsRechargeTransaction sn 
        on sn.MasterSubTransactionId =mst.Id 
        where mt2.TenantDetailId = 1 and
        DATE(sn.CreationTs) BETWEEN '{start_date}' AND '{end_date}' '''
    
   #Reading data from both Server and Excel 
    df_db = pd.read_sql(query, con=engine)
    df_excel = pd.read_excel(CONFIG["excel_file"], dtype=str)

    #replacing the enums to its corresponding status values
    df_db["Recharge_status"]=df_db["Recharge_status"].apply(lambda x: "success" if x == 1 else "pending" if x == 2 else "failed" if x == 3 else "instant failed" if x==4 else x )
    result=filtering_data(df_db,df_excel)
    return result
