import pandas as pd
from db_connector import get_db_connection
from logger_config import logger
from sqlalchemy.exc import SQLAlchemyError

engine = get_db_connection()


# -----------------------------------------------------------------------------
# service function selection
def outward_service_selection(
    start_date, end_date, service_name, transaction_type, df_excel
):
    logger.info(f"Entering Reconciliation for {service_name} Service")
    start_date = start_date
    end_date = end_date

    if service_name == "Recharge":
        df_excel = df_excel.rename(columns={"REFID": "REFID", "DATE": "VEND_DATE"})
        logger.info("Recharge service: Column 'REFID' renamed to 'REFID'")
        tenant_service_id = 160
        Hub_service_id = 7378
        hub_data = recharge_Service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)

    if service_name == "IMT":
        df_excel = df_excel.rename(columns={"REFID": "REFID", "DATE": "VEND_DATE"})
        tenant_service_id = 158
        Hub_service_id = 158
        hub_data = IMT_Service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)
    if service_name == "Pan_UTI":
        df_excel = df_excel.rename()
        tenant_service_id = 4
        Hub_service_id = 4
        hub_data = Panuti_service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)
    if service_name == "BBPS":
        # df_excel = df_excel.rename()
        tenant_service_id = (
            10,
            16,
            22,
            28,
            34,
            40,
            46,
            52,
            58,
            64,
            70,
            76,
            82,
            88,
            94,
            100,
            106,
            112,
            118,
            124,
            130,
            136,
            148,
        )
        Hub_service_id = ",".join(str(x) for x in tenant_service_id)
        tenant_service_id = Hub_service_id
        hub_data = Bbps_service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)

    if service_name == "Pan_NSDL":
        tenant_service_id = 201
        Hub_service_id = 218
        hub_data = Pannsdl_service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)

    if service_name == "Passport":
        tenant_service_id = (
            183,
            184,
            185,
            186,
            187,
            188,
            189,
            190,
            191,
            192,
        )
        Hub_service_id = ",".join(str(x) for x in tenant_service_id)
        tenant_service_id = Hub_service_id
        hub_data = passport_service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)
    return result


# ---------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------
# Filtering Function
def filtering_Data(df_db, df_excel, service_name, tenant_data):
    logger.info(f"Filteration Starts for Inward service {service_name}")
    status_mapping = {
        0: "initiated",
        1: "success",
        2: "failed",
        3: "inprogress",
        4: "partial success",
    }
    columns_to_update = ["IHUB_Master_status", "MasterSubTrans_status"]
    df_db[columns_to_update] = df_db[columns_to_update].apply(
        lambda x: x.map(status_mapping).fillna(x)
    )

    tenant_data["Tenant_status"] = tenant_data["Tenant_status"].apply(
        lambda x: status_mapping.get(x, x)
    )

    def safe_column_select(df, columns):
        existing_cols = [col for col in columns if col in df.columns]
        return df[existing_cols].copy()

    required_columns = [
        "CATEGORY",
        "REFID",
        "VEND_DATE",
        "IHUB_REFERENCE",
        "vendor_reference",
        "UserName",
        "AMOUNT",
        "STATUS",
        "IHUB_Master_status",
        f"{service_name}_status",
        "service_date",
        "Ihub_Ledger_status",
        "Tenant_Ledger_status",
    ]

    not_in_vendor = df_db[~df_db["vendor_reference"].isin(df_excel["REFID"])].copy()
    not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
    not_in_vendor = safe_column_select(not_in_vendor, required_columns)

    # 2. Not in Portal
    not_in_portal = df_excel[~df_excel["REFID"].isin(df_db["vendor_reference"])].copy()
    not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
    not_in_portal = safe_column_select(not_in_portal, required_columns)

    # 3. Vendor success but not in Portal
    not_in_portal_vendor_success = df_excel[
        (~df_excel["REFID"].isin(df_db["vendor_reference"]))
        & (df_excel["STATUS"].str.lower() == "success")
        & (df_db["Ihub_Ledger_status"].str.lower() == "no")
    ]

    not_in_portal_vendor_success["CATEGORY"] = "NOT_IN_PORTAL_VENDOR_SUCCESS"
    not_in_portal_vendor_success = safe_column_select(
        not_in_portal_vendor_success, required_columns
    )

    # 4. Matched
    matched = df_db.merge(
        df_excel, left_on="vendor_reference", right_on="REFID", how="inner"
    ).copy()
    matched["CATEGORY"] = "MATCHED"

    # 5. Mismatched
    mismatched = matched[
        matched[f"{service_name}_status"].str.lower() != matched["STATUS"].str.lower()
    ].copy()
    mismatched["CATEGORY"] = "MISMATCHED"
    mismatched = safe_column_select(mismatched, required_columns)

    # 6. VENDOR_SUCCESS_IHUB_INITIATED
    vendor_success_ihub_initiated = mismatched[
        (mismatched["STATUS"].str.lower() == "success")
        & (mismatched["IHUB_Master_status"].str.lower() == "initiated")
    ].copy()
    vendor_success_ihub_initiated["CATEGORY"] = "VENDOR_SUCCESS_IHUB_INITIATED"
    vendor_success_ihub_initiated = safe_column_select(
        vendor_success_ihub_initiated, required_columns
    )

    # 7. VENDOR_SUCCESS_IHUB_FAILED
    vendor_success_ihub_failed = mismatched[
        (mismatched["STATUS"].str.lower() == "success")
        & (mismatched["IHUB_Master_status"].str.lower() == "failed")
    ].copy()
    vendor_success_ihub_failed["CATEGORY"] = "VENDOR_SUCCESS_IHUB_FAILED"
    vendor_success_ihub_failed = safe_column_select(
        vendor_success_ihub_failed, required_columns
    )

    # 8. VENDOR_FAILED_IHUB_INITIATED
    vendor_failed_ihub_initiated = mismatched[
        (mismatched["STATUS"].str.lower() == "failed")
        & (mismatched["IHUB_Master_status"].str.lower() == "initiated")
    ].copy()
    vendor_failed_ihub_initiated["CATEGORY"] = "VENDOR_FAILED_IHUB_INITIATED"
    vendor_failed_ihub_initiated = safe_column_select(
        vendor_failed_ihub_initiated, required_columns
    )

    vend_ihub_succes_not_in_ledger = matched[
        (matched["STATUS"].str.lower() == "success")
        & (matched["IHUB_Master_status"].str.lower() == "success")
        & (matched["Ihub_Ledger_status"].str.lower() == "no")
    ].copy()
    vend_ihub_succes_not_in_ledger["CATEGORY"] = "VENDOR & IHUB SUCCESS_NOTIN_LEDGER"
    vend_ihub_succes_not_in_ledger = safe_column_select(
        vend_ihub_succes_not_in_ledger, required_columns
    )

    combined = pd.concat(
        [
            not_in_vendor,
            not_in_portal,
            not_in_portal_vendor_success,
            mismatched,
            vendor_success_ihub_initiated,
            vendor_success_ihub_failed,
            vendor_failed_ihub_initiated,
            vend_ihub_succes_not_in_ledger,
            tenant_data,
        ],
        ignore_index=True,
    )

    # Export to Excel
    # combined.to_excel(output_file, index=False)
    logger.info("Filteration Ends")
    return {
        "status": "200",
        "not_in_vendor": not_in_vendor,
        "combined": combined,
        "not_in_Portal": not_in_portal.head(100),
        "mismatched": mismatched,
        "VENDOR_SUCCESS_IHUB_INPROGRESS": vendor_success_ihub_initiated,
        "VENDOR_SUCCESS_IHUB_FAILED": vendor_success_ihub_failed,
        "not_in_Portal_vendor_success": not_in_portal_vendor_success,
        "Vendor_failed_ihub_initiated": vendor_failed_ihub_initiated,
        "vend_ihub_succes_not_in_ledger": vend_ihub_succes_not_in_ledger,
        "Tenant_db_ini_not_in_hubdb": tenant_data,
    }


# ----------------------------------------------------------------------------------
# tenant database filtering function
def tenant_filtering(
    start_date,
    end_date,
    tenant_service_id,
    Hub_service_id,
):
    # To find transaction that is initiated by EBO present in tenant data base But do not hit in hub database
    query = f"""
         WITH cte AS (
         SELECT src.Id, src.UserName , src.TranAmountTotal,src.TransactionStatus as Tenant_status,
         src.CreationTs, src.VendorSubServiceMappingId,hub.Id AS hub_id,hub.VendorSubServiceMappingId AS HVM_id
         FROM (
         SELECT mt.*,u.UserName  FROM tenantinetcsc.MasterTransaction mt left join tenantinetcsc.EboDetail ed on ed.id = mt.EboDetailId
         left join tenantinetcsc.`User` u  on u.Id = ed.UserId
         WHERE DATE(mt.CreationTs) BETWEEN '{start_date}' AND '{end_date}'
         AND mt.VendorSubServiceMappingId in ({tenant_service_id})
         UNION ALL
         SELECT umt.*,u.UserName FROM tenantupcb.MasterTransaction umt left join tenantupcb.EboDetail ed on ed.id = umt.EboDetailId
         left join tenantupcb.`User` u  on u.Id = ed.UserId
         WHERE DATE(umt.CreationTs) BETWEEN '{start_date}' AND '{end_date}'
         AND umt.VendorSubServiceMappingId in  ({tenant_service_id})
         UNION ALL
         SELECT imt.*,u.UserName FROM tenantiticsc.MasterTransaction imt  left join tenantiticsc.EboDetail ed on ed.id = imt.EboDetailId
         left join tenantiticsc.`User` u  on u.Id = ed.UserId
         WHERE DATE(imt.CreationTs) BETWEEN '{start_date}' AND '{end_date}'
         AND imt.VendorSubServiceMappingId in  ({tenant_service_id})
         ) AS src
         LEFT JOIN ihubcore.MasterTransaction AS hub
         ON hub.TenantMasterTransactionId = src.Id
         AND hub.TenantDetailId = 1
         AND DATE(hub.CreationTs) BETWEEN '{start_date}' AND '{end_date}'
         AND hub.VendorSubServiceMappingId in ({Hub_service_id})
         )
         SELECT *
         FROM cte
         WHERE hub_id IS NULL"""
    print(query)
    try:
        with engine.begin() as connection:
            # result has the record for data that trigerred in tenant but not hit hub
            result = pd.read_sql(query, con=engine)

    except SQLAlchemyError as e:
        logger.error(f"Database error in recharge_Service(): {e}")
    return result


# -----------------------------------------------------------------------------
# Recharge service function
def recharge_Service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f"""
            SELECT mt2.TransactionRefNum AS Ihub_reference, sn.requestID AS vendor_reference,
            u.UserName, mt2.TransactionStatus AS IHUB_Master_status, mst.TransactionStatus AS MasterSubTrans_status,
            sn.CreationTs AS service_date, sn.rechargeStatus AS {service_name}_status,
            CASE
                WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                ELSE 'No'
            END AS Ihub_Ledger_status,
            CASE
                WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                ELSE 'No'
            END AS Tenant_Ledger_status
            FROM
            ihubcore.MasterTransaction mt2
        LEFT JOIN ihubcore.MasterSubTransaction mst
            ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN ihubcore.PsRechargeTransaction sn
            ON sn.MasterSubTransactionId = mst.Id
        LEFT JOIN tenantinetcsc.EboDetail ed
            ON mt2.EboDetailId = ed.Id
        LEFT JOIN tenantinetcsc.`User` u
            ON u.Id = ed.UserId
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN '{start_date}' AND CURRENT_DATE()
        ) iwt ON iwt.IHubReferenceId = mt2.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN '{start_date}' AND CURRENT_DATE()
        ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum
        WHERE
            DATE(sn.CreationTs) BETWEEN '{start_date}' AND '{end_date}'
        """
    try:
        with engine.begin() as connection:
            # Reading data from Server
            df_db = pd.read_sql(query, con=connection)
            # replacing the enums to its corresponding status values
            status_mapping = {
                0: "initiated",
                1: "success",
                2: "pending",
                3: "failed",
                4: "instant failed",
            }
            df_db[f"{service_name}_status"] = df_db[f"{service_name}_status"].apply(
                lambda x: status_mapping.get(x, x)
            )
            result = df_db
    except SQLAlchemyError as e:
        logger.error(f"Database error in recharge_Service(): {e}")
    return result


# ---------------------------------------------------------------------------------------
# IMT SERVICE FUNCTION
def IMT_Service(start_date, end_date, df_excel, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f"""
            SELECT mt2.TransactionRefNum AS Ihub_reference,
            pst.VendorReferenceId as vendor_reference,
            u.UserName,
            mt2.TransactionStatus AS IHUB_Master_status,
            mst.TransactionStatus AS MasterSubTrans_status,
            pst.PaySprintTransStatus as {service_name}_status,
            CASE
            WHEN a.IHubReferenceId  IS NOT NULL THEN 'Yes'
            ELSE 'No'
            END AS Ihub_Ledger_status
            FROM
            ihubcore.MasterTransaction mt2
            LEFT JOIN
            ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt2.Id
            LEFT JOIN
            ihubcore.PaySprint_Transaction pst ON pst.MasterSubTransactionId = mst.Id
            LEFT JOIN
            tenantinetcsc.EboDetail ed ON mt2.EboDetailId = ed.Id
            LEFT JOIN
            tenantinetcsc.`User` u ON u.id = ed.UserId
            LEFT JOIN
            (SELECT DISTINCT iwt.IHubReferenceId AS IHubReferenceId
            FROM ihubcore.IHubWalletTransaction iwt
            WHERE DATE(iwt.CreationTs) BETWEEN '{start_date}' AND CURRENT_DATE()
            ) a ON a.IHubReferenceId = mt2.TransactionRefNum
            WHERE
            DATE(pst.CreationTs) BETWEEN '{start_date}' AND '{end_date}' 
            """
    # Reading data from Server
    df_db = pd.read_sql(query, con=engine)
    df_db["vendor_reference"] = df_db["vendor_reference"].astype(str)
    df_excel["REFID"] = df_excel["REFID"].astype(str)
    refunded_trans_ids = df_excel[df_excel["STATUS"].isin(["Refunded", "Failed"])]
    # print(refunded_trans_ids)
    # Extract the REFID column as a list of strings (safely handling quotes)
    refunded_ids_list = refunded_trans_ids["REFID"].astype(str).tolist()
    # Joining the list into a properly quoted string for SQL
    # refunded_ids_string = "\n   UNION ALL\n   ".join(
    # f"SELECT '{refid}' AS TransactionRefNumVendor" for refid in refunded_ids_list)
    refunded_ids_string = ",".join(f"'{refid}'" for refid in refunded_ids_list)
    # print(refunded_ids_string)

    query = f"""
        SELECT pst.VendorReferenceId,
        CASE WHEN mr.MasterSubTransactionId IS NOT NULL THEN 'refunded'
        ELSE 'not_refunded'
        END AS Ihub_refund_status
        FROM ihubcore.PaySprint_Transaction pst
        LEFT JOIN ihubcore.MasterRefund mr
        ON mr.MasterSubTransactionId = pst.MasterSubTransactionId
        WHERE pst.VendorReferenceId IN ({refunded_ids_string})
        AND DATE(pst.creationTs) BETWEEN "{start_date}" AND "{end_date}"
        """
    # print(query)
    refunded_db = pd.read_sql(query, con=engine)
    # print(refunded_db)
    refunded_db["VendorReferenceId"] = refunded_db["VendorReferenceId"].astype(str)
    merged_df = df_db.merge(
        refunded_db,
        how="left",
        left_on="vendor_reference",
        right_on="VendorReferenceId",
    )
    merged_df.drop(columns=["VendorReferenceId"], inplace=True)
    df_db = merged_df
    df_db["Ihub_refund_status"] = df_db["Ihub_refund_status"].fillna("not_applicable")
    # print(df_db)
    # df_db.to_excel("C:\\Users\\Sathyaswaruban\\Documents\\IMT.xlsx", index=False)
    # df_excel['STATUS'] = df_excel['STATUS'].replace('Refunded', 'failed')
    logger.info(
        "Refunded status in IMT excel renamed to failed since IHUB portal don't have refunded status"
    )
    # mapping status name with enums
    status_mapping = {
        0: "unknown",
        1: "success",
        2: "failed",
        3: "inprogress",
        4: "partialsuccuess",
    }
    df_db[f"{service_name}_status"] = df_db[f"{service_name}_status"].apply(
        lambda x: status_mapping.get(x, x)
    )
    # calling filtering function
    result = filtering_Data(df_db, df_excel, service_name)
    return result


# -------------------------------------------------------------------
# BBPS FUNCTION
def Bbps_service(start_date, end_date, df_excel, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f"""
        SELECT
        mt2.TransactionRefNum as Ihub_reference,
        u.Username,
        bbp.TxnRefId  as vendor_reference,
        mt2.TransactionStatus AS HUB_Master_status,
        mst.TransactionStatus AS MasterSubTrans_status,
        bbp.TransactionStatusType ,u.UserName,bbp.HeadReferenceId ,
        CASE when iw.IHubReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS IhubLedger_status,
        CASE when bf.HeadReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS Billfetch_status
        FROM  ihubcore.MasterTransaction mt2
        LEFT JOIN 
        ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN 
        ihubcore.BBPS_BillPay bbp ON bbp.MasterSubTransactionId = mst.Id 
        left join tenantinetcsc.EboDetail ed on ed.Id = mt2.EboDetailId 
        left join tenantinetcsc.`User` u  on u.id = ed.UserId 
        left join (Select DISTINCT iwt.IHubReferenceId from ihubcore.IHubWalletTransaction iwt 
        where date(iwt.creationTs) between '{start_date}' and CURRENT_DATE() ) as iw 
        on iw.IHubReferenceId =mt2.TransactionRefNum 
        left join (select DISTINCT bbf.HeadReferenceId  from ihubcore.BBPS_BillFetch bbf 
        where date(bbf.creationTs) between '{start_date}' and current_date()) as bf 
        on bf.HeadReferenceId  = bbp.HeadReferenceId
        WHERE DATE(bbp.CreationTs) BETWEEN '{start_date}' AND '{end_date}' 
        """
    # Reading data from Server
    df_db = pd.read_sql(query, con=engine)
    # mapping status name with enum
    status_mapping = {
        0: "unknown",
        1: "success",
        2: "failed",
        3: "inprogress",
        4: "partialsuccuess",
    }
    df_db[f"{service_name}_status"] = df_db[f"{service_name}_status"].apply(
        lambda x: status_mapping.get(x, x)
    )
    # calling filtering function
    result = filtering_Data(df_db, df_excel, service_name)
    return result


# ------------------------------------------------------------------------
# PAN-UTI Service function
def Panuti_service(start_date, end_date, df_excel, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f"""
        select u.ApplicationNumber as vendor_reference,
        u.UTITSLTransID_Gateway as UTITSLTrans_id,
        mt.TransactionRefNum AS Ihub_reference,
        mt.TransactionStatus AS IHUB_Master_status,
        mst.TransactionStatus AS MasterSubTrans_status,
        u.TransactionStatusType as {service_name}_status,
        CASE When 
        a.IHubReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS 'Ihub_Ledger_status'
        from ihubcore.UTIITSLTTransaction u 
        left join ihubcore.MasterSubTransaction mst on mst.Id  = u.MasterSubTransactionId
        left join ihubcore.MasterTransaction mt on mt.Id  = mst.MasterTransactionId
        left join tenantinetcsc.EboDetail ed on ed.Id = mt.EboDetailId 
        left join tenantinetcsc.`User` u  on u.id = ed.UserId 
        left join (select DISTINCT iwt.IHubReferenceId  from ihubcore.IHubWalletTransaction iwt  
        WHERE Date(iwt.creationTs) BETWEEN '{start_date}'AND CURRENT_DATE()) a 
        on a.IHubReferenceId = mt.TransactionRefNum
        where DATE(u.CreationTs) BETWEEN '{start_date}' and '{end_date}
        """
    # Reading data from Server
    df_db = pd.read_sql(query, con=engine)
    # mapping status name with enum
    status_mapping = {
        0: "initiated",
        1: "success",
        2: "failed",
        3: "inprogress",
        4: "partial success",
    }
    df_db[f"{service_name}_status"] = df_db[f"{service_name}_status"].apply(
        lambda x: status_mapping.get(x, x)
    )
    # calling filtering function
    result = filtering_Data(df_db, df_excel, service_name)
    return result


# ------------------------------------------------------------------------
# PAN-NSDL Service function
def Pannsdl_service(start_date, end_date, df_excel, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f"""
        select pit.AcknowledgeNo as vendor_reference,mt.TransactionRefNum AS Ihub_reference,
        mt.TransactionStatus AS IHUB_Master_status,
        mst.TransactionStatus AS MasterSubTrans_status,
        u.Username,
        pit.applicationstatus as {service_name}_status,
        CASE When 
        a.IHubReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS 'Ihub_Ledger_status'
        from ihubcore.PanInTransaction pit
        left join ihubcore.MasterSubTransaction mst on mst.id= pit.MasterSubTransactionId
        left join ihubcore.MasterTransaction mt on mt.id = mst.MasterTransactionId
        left join tenantinetcsc.EboDetail ed on ed.Id = mt.EboDetailId 
        left join tenantinetcsc.`User` u  on u.id = ed.UserId 
        left join (select DISTINCT iwt.IHubReferenceId  from ihubcore.IHubWalletTransaction iwt  
        WHERE Date(iwt.creationTs) BETWEEN '{start_date}'AND CURRENT_DATE()) a 
        on a.IHubReferenceId = mt.TransactionRefNum
        where DATE(u.CreationTs) BETWEEN '{start_date}' and '{end_date}
        """
    # Reading data from Server
    df_db = pd.read_sql(query, con=engine)
    # mapping status name with enum
    status_mapping = {
        0: "None",
        1: "New",
        2: "Acknowledged",
        3: "Rejected",
        4: "Uploaded",
        5: "Processed",
        6: "Reupload",
        7: "Alloted",
        8: "Objection",
        9: "MoveToNew",
    }
    df_db[f"{service_name}_status"] = df_db[f"{service_name}_status"].apply(
        lambda x: status_mapping.get(x, x)
    )
    # calling filtering function
    result = filtering_Data(df_db, df_excel, service_name)
    return result


# ------------------------------------------------------------------------------
# passport service function
def passport_service(start_date, end_date, service_name):
    # need to query
    result = 0
    return result
