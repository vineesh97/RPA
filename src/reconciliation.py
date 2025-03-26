import pandas as pd
from db_connector import get_db_connection
from logger_config import logger
engine = get_db_connection()

def run_Reconciliation(start_date,end_date,service_name,df_excel):
    logger.info(f"Entering Reconciliation for {service_name} Service")
    start_date=start_date
    end_date=end_date

    if service_name=='Recharge':
        result = recharge_Service(start_date,end_date,df_excel,service_name)
    if service_name=='Aeps':
        result = aeps_Service(start_date,end_date,df_excel,service_name)
    return result


def filtering_Data (df_db,df_excel,service_name):
    logger.info("Filteration Starts")
    status_mapping = {
    1: "success",
    2: "pending",
    3: "failed",
    4: "instant failed"
    }
    columns_to_update = ["HUB_Master_status", "MasterSubTrans_status","Tenant_Status"]
    df_db[columns_to_update] = df_db[columns_to_update].apply(lambda x:x.map(status_mapping).fillna(x))

    #df_db.to_excel("D:\\GitHub\\RPA\\rpa_reconciliation\\rpa_reconciliation\\data\\Test.xlsx",index=False)
    #Seperating data not present in vendor_statement but present in I HUB Database 
    not_in_vendor= df_db[~df_db["vendor_reference"].isin(df_excel["REFID"])][["vendor_reference", f'{service_name}_status']]
    
    #Seperating data present in vendor_statement but NOT present in I HUB Database 
    not_in_Portal= df_excel[~df_excel["REFID"].isin(df_db["vendor_reference"])][["REFID", "USERNAME", "AMOUNT", "STATUS", "DATE"]]
    
    #Seperating data  matching in vendor_statement and  in I HUB Database 
    matched = df_db.merge(df_excel, left_on="vendor_reference", right_on="REFID", how="inner")
    
    #Seperating data matching in both but having diff status value 
    mismatched = matched[matched[f'{service_name}_status'].str.lower() != matched["STATUS"].str.lower()]

   #VENDOR_SUCCESS_IHUB_INPROGRESS
    Vendor_Success_ihub_inprogess = mismatched[
            (mismatched['STATUS'] == 'Success') &  # STATUS in df_recharge_excel is Success
            (mismatched[f'{service_name}_status'] == "success") &  # Recharge_status in df1 is Success
            (mismatched['HUB_Master_status'] == "In progress" )  # Mst_status in df1 is NOT Success
        ]
   #VENDOR_SUCCESS_IHUB_FAILED___
    Vendor_Success_ihub_failed = mismatched[
            (mismatched['STATUS'] == 'Success') &  # STATUS in df_recharge_excel is Success
            (mismatched[f'{service_name}_status'] == "success") &  # Recharge_status in df1 is Success
            (mismatched['HUB_Master_status'] == "failed" )  # Mst_status in df1 is NOT Success
        ]
    logger.info("Filteration Ends")
    return {"status":"200","not_in_vendor": not_in_vendor, "not_in_Portal": not_in_Portal.head(100), "mismatched": mismatched,"VENDOR_SUCCESS_IHUB_INPROGRESS":Vendor_Success_ihub_inprogess ,"VENDOR_SUCCESS_IHUB_FAILED":Vendor_Success_ihub_failed}  

#Recharge service function
def recharge_Service(start_date, end_date,df_excel,service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f'''
        select mt2.TransactionRefNum ,sn.requestID as vendor_reference ,mt.TransactionStatus  as Tenant_Status,u.UserName ,mt2.TransactionStatus  as HUB_Master_status,
        mst.TransactionStatus as MasterSubTrans_status,sn.rechargeStatus  as  {service_name}_status
        from iHubTenantPortal_dev.MasterTransaction mt
        left join ihub_dev.MasterTransaction mt2
        on mt.Id =mt2.TenantMasterTransactionId
        left join ihub_dev.MasterSubTransaction mst
        on mst.MasterTransactionId  =mt2.Id
        left join ihub_dev.PsRechargeTransaction sn
        on sn.MasterSubTransactionId =mst.Id
        left join iHubTenantPortal_dev.EboDetail ed
        on mt.EboDetailId =ed.Id
        left join iHubTenantPortal_dev.`User` u
        on u.id=ed.UserId
        where mt2.TenantDetailId = 1 and
        DATE(sn.CreationTs) BETWEEN '{start_date}' AND '{end_date}' '''
    
    #Reading data from Server
    df_db = pd.read_sql(query, con=engine)
    #print(df_db.columns)

    #replacing the enums to its corresponding status values
    status_mapping = {
    1: "success",
    2: "pending",
    3: "failed",
    4: "instant failed"
    }
    df_db[f"{service_name}_status"] = df_db[f"{service_name}_status"].apply(lambda x: status_mapping.get(x, x))
    result=filtering_Data(df_db,df_excel,service_name)
    return result

def aeps_Service(start_date, end_date,df_excel,service_name):
    logger.info(f"Fetching data from HUB for {service_name}") 
    query = f'''
            select mt2.TransactionRefNum ,pat.requestID as vendor_reference ,mt.TransactionStatus as Tenant_Status,u.UserName ,mt2.TransactionStatus as HUB_Master_status,
            mst.TransactionStatus as MasterSubTrans_status,pat.TransStatus as {service_name}_status
            from iHubTenantPortal_dev.MasterTransaction mt
            left join ihub_dev.MasterTransaction mt2
            on mt.Id =mt2.TenantMasterTransactionId
            left join ihub_dev.MasterSubTransaction mst
            on mst.MasterTransactionId=mt2.Id
            left join ihub_dev.PsAepsTransaction pat
            on pat.MasterSubTransactionId =mst.Id
            left join iHubTenantPortal_dev.EboDetail ed
            on mt.EboDetailId =ed.Id
            left join iHubTenantPortal_dev.`User` u 
            on u.id=ed.UserId
            where mt2.TenantDetailId = 1 and
            DATE(pat.CreationTs) BETWEEN '{start_date}' AND '{end_date}' '''
    #Reading data from Server 
    df_db = pd.read_sql(query, con=engine)
    #mapping status name with enum
    status_mapping = {
    1: "success",
    2: "pending",
    3: "failed",
    4: "instant failed"
    }
    df_db[f"{service_name}_status"] = df_db[f"{service_name}_status"].apply(lambda x: status_mapping.get(x, x))
    #calling filtering function
    result=filtering_Data(df_db,df_excel,service_name)
    return result
