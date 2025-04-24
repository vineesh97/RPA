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
    0: "initiated",
    1: "success",
    2: "failed",
    3: "inprogress",
    4: "partial success",
    }
    columns_to_update = ["HUB_Master_status", "MasterSubTrans_status","Tenant_Status"]
    df_db[columns_to_update] = df_db[columns_to_update].apply(lambda x:x.map(status_mapping).fillna(x))
 
    not_in_vendor = df_db[~df_db["vendor_reference"].isin(df_excel["REFID"])][["vendor_reference", "service_date", f'{service_name}_status']].copy()
    not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
 
    # 2. Not in Portal
    not_in_portal = df_excel[~df_excel["REFID"].isin(df_db["vendor_reference"])][["REFID", "USERNAME", "AMOUNT", "STATUS", "DATE"]].copy()
    not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
 
    # 3. Vendor success but not in Portal
    not_in_portal_vendor_success = df_excel[
        (~df_excel["REFID"].isin(df_db["vendor_reference"])) &
        (df_excel["STATUS"].str.lower() == "success")
    ][["REFID", "USERNAME", "AMOUNT", "STATUS", "DATE"]].copy()
    not_in_portal_vendor_success["CATEGORY"] = "NOT_IN_PORTAL_VENDOR_SUCCESS"
 
    # 4. Matched
    matched = df_db.merge(df_excel, left_on="vendor_reference", right_on="REFID", how="inner").copy()
    matched["CATEGORY"] = "MATCHED"
 
    # 5. Mismatched
    mismatched = matched[matched[f'{service_name}_status'].str.lower() != matched["STATUS"].str.lower()].copy()
    mismatched["CATEGORY"] = "MISMATCHED"
 
    # 6. VENDOR_SUCCESS_IHUB_INITIATED
    vendor_success_ihub_initiated = mismatched[
        (mismatched['STATUS'].str.lower() == 'success') &
        (mismatched['HUB_Master_status'].str.lower() == "initiated")
    ].copy()
    vendor_success_ihub_initiated["CATEGORY"] = "VENDOR_SUCCESS_IHUB_INITIATED"
 
    # 7. VENDOR_SUCCESS_IHUB_FAILED
    vendor_success_ihub_failed = mismatched[
        (mismatched['STATUS'].str.lower() == 'success') &
        (mismatched['HUB_Master_status'].str.lower() == "failed")
    ].copy()
    vendor_success_ihub_failed["CATEGORY"] = "VENDOR_SUCCESS_IHUB_FAILED"
 
    # 8. VENDOR_FAILED_IHUB_INITIATED
    vendor_failed_ihub_initiated = mismatched[
        (mismatched['STATUS'].str.lower() == 'failed') &
        (mismatched['HUB_Master_status'].str.lower() == "initiated")
    ].copy()
    vendor_failed_ihub_initiated["CATEGORY"] = "VENDOR_FAILED_IHUB_INITIATED"
 
    combined = pd.concat([
        not_in_vendor,
        not_in_portal,
        not_in_portal_vendor_success,
        mismatched,
        vendor_success_ihub_initiated,
        vendor_success_ihub_failed,
        vendor_failed_ihub_initiated,
        matched
    ], ignore_index=True)
 
    # Export to Excel
    #combined.to_excel(output_file, index=False)
    logger.info("Filteration Ends")
    return {
        "status":"200",
        "not_in_vendor": not_in_vendor,
        "combined":combined,
        "not_in_Portal":not_in_portal.head(100),
        "mismatched": mismatched,
        "VENDOR_SUCCESS_IHUB_INPROGRESS":vendor_success_ihub_initiated,
        "VENDOR_SUCCESS_IHUB_FAILED":vendor_success_ihub_failed,
        "not_in_Portal_vendor_success": not_in_portal_vendor_success,
        "Vendor_failed_ihub_initiated":vendor_failed_ihub_initiated,
        "matched": matched.head(100)}
 
#Recharge service function
def recharge_Service(start_date, end_date,df_excel,service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f'''
         select mt2.TransactionRefNum ,sn.requestID as vendor_reference ,mt.TransactionStatus  as Tenant_Status,u.UserName ,
 mt2.TransactionStatus  as HUB_Master_status, mst.TransactionStatus as MasterSubTrans_status, sn.CreationTs as service_date,sn.rechargeStatus  as  {service_name}_status
        from tenantinetcsc.MasterTransaction mt
        left join ihubcore.MasterTransaction mt2
        on mt.Id =mt2.TenantMasterTransactionId
        left join ihubcore.MasterSubTransaction mst
        on mst.MasterTransactionId  =mt2.Id
        left join ihubcore.PsRechargeTransaction sn
        on sn.MasterSubTransactionId =mst.Id
        left join tenantinetcsc.EboDetail ed
        on mt.EboDetailId =ed.Id
        left join tenantinetcsc.`User` u
        on u.id=ed.UserId
        where mt2.TenantDetailId = 1 and
        DATE(sn.CreationTs) BETWEEN '{start_date}' AND '{end_date}' '''
   
    #Reading data from Server
    df_db = pd.read_sql(query, con=engine)
    #print(df_db.columns)
 
    #replacing the enums to its corresponding status values
    status_mapping = {
    0: "initiated",
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
            from tenantinetcsc.MasterTransaction mt
            left join ihubcore.MasterTransaction mt2
            on mt.Id =mt2.TenantMasterTransactionId
            left join ihubcore.MasterSubTransaction mst
            on mst.MasterTransactionId=mt2.Id
            left join ihubcore.PsAepsTransaction pat
            on pat.MasterSubTransactionId =mst.Id
            left join tenantinetcsc.EboDetail ed
            on mt.EboDetailId =ed.Id
            left join tenantinetcsc.`User` u
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