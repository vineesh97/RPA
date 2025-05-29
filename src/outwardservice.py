import pandas as pd
from db_connector import get_db_connection
from logger_config import logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from sqlalchemy.exc import OperationalError, DatabaseError

engine = get_db_connection()

# Configure retry logic for database operations
DB_RETRY_CONFIG = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=1, min=1, max=10),
    "retry": retry_if_exception_type((OperationalError, DatabaseError)),
    "reraise": True,
}


@retry(**DB_RETRY_CONFIG)
def execute_sql_with_retry(connection, query, params=None):
    logger.info("Entered helper function to execute SQL with retry logic")
    return pd.read_sql(query, con=connection, params=params)


# -----------------------------------------------------------------------------
# service function selection
def outward_service_selection(start_date, end_date, service_name, df_excel):
    logger.info(f"Entering Reconciliation for {service_name} Service")
    start_date = start_date
    end_date = end_date
    result = None

    if service_name == "RECHARGE":
        df_excel = df_excel.rename(columns={"REFID": "REFID", "DATE": "VENDOR_DATE"})
        logger.info("Recharge service: Column 'REFID' renamed to 'REFID'")
        tenant_service_id = 160
        Hub_service_id = (
            7378,
            7379,
        )
        Hub_service_id = ",".join(str(x) for x in Hub_service_id)
        hub_data = recharge_Service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)

    elif service_name == "IMT":
        df_excel = df_excel.rename(columns={"REFID": "REFID", "DATE": "VENDOR_DATE"})
        tenant_service_id = 158
        Hub_service_id = 158
        hub_data = IMT_Service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)
    elif service_name == "Pan_UTI":
        df_excel = df_excel.rename()
        tenant_service_id = 4
        Hub_service_id = 4
        hub_data = Panuti_service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)
    elif service_name == "BBPS":
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

    elif service_name == "Pan_NSDL":
        tenant_service_id = 201
        Hub_service_id = 218
        hub_data = Pannsdl_service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)

    elif service_name == "PASSPORT":
        Hub_service_id = (
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
        Hub_service_id = ",".join(str(x) for x in Hub_service_id)
        tenant_service_id = (
            166,
            167,
            168,
            169,
            170,
            171,
            172,
            173,
            174,
            175,
        )
        tenant_service_id = ",".join(str(x) for x in tenant_service_id)
        hub_data = passport_service(start_date, end_date, service_name)
        tenant_data = tenant_filtering(
            start_date, end_date, tenant_service_id, Hub_service_id
        )
        result = filtering_Data(hub_data, df_excel, service_name, tenant_data)
    else:
        message = "Error in selecting outward service function.Kindly check your service value..!"
        return message
    return result


# ---------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------
# Filtering Function
def filtering_Data(df_db, df_excel, service_name, tenant_data):
    logger.info(f"Filteration Starts for {service_name} service")

    mapping = None
    # converting the date of both db and excel to string
    df_db["SERVICE_DATE"] = df_db["SERVICE_DATE"].dt.strftime("%Y-%m-%d")
    df_excel["VENDOR_DATE"] = pd.to_datetime(
        df_excel["VENDOR_DATE"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # Mapping names with corresponding values
    status_mapping = {
        0: "initiated",
        1: "success",
        2: "failed",
        3: "inprogress",
        4: "partial success",
    }

    columns_to_update = ["IHUB_MASTER_STATUS"]
    df_db[columns_to_update] = df_db[columns_to_update].apply(
        lambda x: x.map(status_mapping).fillna(x)
    )

    tenant_data["TENANT_STATUS"] = tenant_data["TENANT_STATUS"].apply(
        lambda x: status_mapping.get(x, x)
    )

    # Renaming Col in Excel
    df_excel = df_excel.rename(columns={"STATUS": "VENDOR_STATUS"})

    # function to select only required cols and make it as df
    def safe_column_select(df, columns):
        existing_cols = [col for col in columns if col in df.columns]
        return df[existing_cols].copy()

    # Required columns that to be sent as result to UI
    required_columns = [
        "CATEGORY",
        "VENDOR_DATE",
        "TENANT_ID",
        "IHUB_REFERENCE",
        "REFID",
        "IHUB_USERNAME",
        "AMOUNT",
        "VENDOR_STATUS",
        "IHUB_MASTER_STATUS",
        f"{service_name}_STATUS",
        "SERVICE_DATE",
        "IHUB_LEDGER_STATUS",
        # "TENANT_LEDGER_STATUS",
        "TRANSACTION_CREDIT",
        "TRANSACTION_DEBIT",
        "COMMISSION_CREDIT",
        "COMMISSION_REVERSAL",
    ]
    # req_col_for_not_in_portal = [
    #     "CATEGORY",
    #     "VENDOR_DATE",
    #     "IHUB_REFERENCE",
    #     "REFID",
    #     "AMOUNT",
    #     "VENDOR_STATUS",
    # ]

    # 1 Filtering Data initiated in IHUB portal and not in Vendor Xl
    not_in_vendor = df_db[~df_db["VENDOR_REFERENCE"].isin(df_excel["REFID"])].copy()
    not_in_vendor["CATEGORY"] = "NOT_IN_VENDOR"
    not_in_vendor = safe_column_select(not_in_vendor, required_columns)

    # 2. Filtering Data Present in Vendor XL but Not in Ihub Portal
    not_in_portal = df_excel[~df_excel["REFID"].isin(df_db["VENDOR_REFERENCE"])].copy()
    not_in_portal["CATEGORY"] = "NOT_IN_PORTAL"
    not_in_portal = safe_column_select(not_in_portal, required_columns)

    # # 3. Vendor success but not in Portal
    # not_in_portal_vendor_success = df_excel[
    #     (~df_excel["REFID"].isin(df_db["VENDOR_REFERENCE"]))
    #     & (df_excel["VENDOR_STATUS"].str.lower() == "success")
    #     & (df_db["IHUB_LEDGER_STATUS"].str.lower() == "no")
    # ]

    # not_in_portal_vendor_success["CATEGORY"] = "NOT_IN_PORTAL_VENDOR_SUCCESS"
    # not_in_portal_vendor_success = safe_column_select(
    #     not_in_portal_vendor_success, required_columns
    # )

    # 4. Filtering Data that matches in both Ihub Portal and Vendor Xl as : Matched
    matched = df_db.merge(
        df_excel, left_on="VENDOR_REFERENCE", right_on="REFID", how="inner"
    ).copy()
    matched["CATEGORY"] = "MATCHED"
    matched = safe_column_select(matched, required_columns)
    # print(matched.columns)

    # 5. Filtering Data that Mismatched in both Ihub Portal and Vendor Xl as : Mismatched
    mismatched = matched[
        matched[f"{service_name}_STATUS"].str.lower()
        != matched["VENDOR_STATUS"].str.lower()
    ].copy()
    mismatched["CATEGORY"] = "MISMATCHED"
    mismatched = safe_column_select(mismatched, required_columns)

    # 6. Getting total count of success and failure data
    matched_success_status = matched[
        (matched[f"{service_name}_STATUS"].str.lower() == "success")
        & (matched["VENDOR_STATUS"].str.lower() == "success")
    ]

    success_count = matched_success_status.shape[0]
    matched_failed_status = matched[
        (matched[f"{service_name}_STATUS"].str.lower() == "failed")
        & (matched["VENDOR_STATUS"].str.lower() == "failed")
    ]
    failed_count = matched_failed_status.shape[0]

    # Scearios Blocks Based on Not In Ledger (NIL) and In Ledger ```````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````
    # SCENARIO 1 VEND_IHUB_SUC-NIL
    vend_ihub_succ_not_in_ledger = matched[
        (matched["VENDOR_STATUS"].str.lower() == "success")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "success")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
    ].copy()
    vend_ihub_succ_not_in_ledger["CATEGORY"] = "VEND_IHUB_SUC-NIL"
    vend_ihub_succ_not_in_ledger = safe_column_select(
        vend_ihub_succ_not_in_ledger, required_columns
    )
    # SCENARIO 2 VEND_FAIL_IHUB_SUC-NIL
    vend_fail_ihub_succ_not_in_ledger = matched[
        (matched["VENDOR_STATUS"].str.lower() == "failed")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "success")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
    ].copy()
    vend_fail_ihub_succ_not_in_ledger["CATEGORY"] = "VEND_FAIL_IHUB_SUC-NIL"
    vend_fail_ihub_succ_not_in_ledger = safe_column_select(
        vend_fail_ihub_succ_not_in_ledger, required_columns
    )
    # SCENARIO 3 VEND_SUC_IHUB_FAIL-NIL
    vend_succ_ihub_fail_not_in_ledger = matched[
        (matched["VENDOR_STATUS"].str.lower() == "success")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "failed")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
    ].copy()
    vend_succ_ihub_fail_not_in_ledger["CATEGORY"] = "VEND_SUC_IHUB_FAIL-NIL"
    vend_succ_ihub_fail_not_in_ledger = safe_column_select(
        vend_succ_ihub_fail_not_in_ledger, required_columns
    )
    # SCENARIO 4 IHUB_FAIL_VEND_FAIL-NIL
    #    ihub_vend_fail_not_in_ledger = matched[
    #        (matched["VENDOR_STATUS"].str.lower() == "failed")
    #        & (matched["IHUB_MASTER_STATUS"].str.lower() == "failed")
    #        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
    #    ].copy()
    #    ihub_vend_fail_not_in_ledger["CATEGORY"] = "IHUB_FAIL_VEND_FAIL-NIL"
    #    ihub_vend_fail_not_in_ledger = safe_column_select(
    #         ihub_vend_fail_not_in_ledger, required_columns
    #    )
    # SCENARIO 5 IHUB_INT_VEND_SUC-NIL
    ihub_initiate_vend_succes_not_in_ledger = matched[
        (matched["VENDOR_STATUS"].str.lower() == "success")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "initiated")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
    ].copy()
    ihub_initiate_vend_succes_not_in_ledger["CATEGORY"] = "IHUB_INT_VEND_SUC-NIL"
    ihub_initiate_vend_succes_not_in_ledger = safe_column_select(
        ihub_initiate_vend_succes_not_in_ledger, required_columns
    )
    # SCENARIO 6 VEND_FAIL_IHUB_INT-NIL
    ihub_initiate_vend_fail_not_in_ledger = matched[
        (matched["VENDOR_STATUS"].str.lower() == "failed")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "initiated")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
    ].copy()
    ihub_initiate_vend_fail_not_in_ledger["CATEGORY"] = " VEND_FAIL_IHUB_INT-NIL"
    ihub_initiate_vend_fail_not_in_ledger = safe_column_select(
        ihub_initiate_vend_fail_not_in_ledger, required_columns
    )

    # SCENARIO 1 VEND_IHUB_SUC IL
    #    vend_ihub_succ = matched[
    #        (matched["VENDOR_STATUS"].str.lower() == "success")
    #        & (matched["IHUB_MASTER_STATUS"].str.lower() == "success")
    #        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
    #    ].copy()
    #    vend_ihub_succ["CATEGORY"] = "VEND_IHUB_SUC"
    #    vend_ihub_succ = safe_column_select(
    #         vend_ihub_succ, required_columns
    #    )
    # SCENARIO 2 VEND_FAIL_IHUB_SUC IL
    vend_fail_ihub_succ = matched[
        (matched["VENDOR_STATUS"].str.lower() == "failed")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "success")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
    ].copy()
    vend_fail_ihub_succ["CATEGORY"] = "VEND_FAIL_IHUB_SUC"
    vend_fail_ihub_succ = safe_column_select(vend_fail_ihub_succ, required_columns)
    # SCENARIO 3 VEND_SUC_IHUB_FAIL
    vend_succ_ihub_fail = matched[
        (matched["VENDOR_STATUS"].str.lower() == "success")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "failed")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
    ].copy()
    vend_succ_ihub_fail["CATEGORY"] = "VEND_SUC_IHUB_FAIL"
    vend_succ_ihub_fail = safe_column_select(vend_succ_ihub_fail, required_columns)
    # SCENARIO 4 IHUB_VEND_FAIL IL
    ihub_vend_fail = matched[
        (matched["VENDOR_STATUS"].str.lower() == "failed")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "failed")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
    ].copy()
    ihub_vend_fail["CATEGORY"] = "IHUB_VEND_FAIL"
    ihub_vend_fail = safe_column_select(ihub_vend_fail, required_columns)
    # SCENARIO 5 IHUB_INT_VEND_SUC IL
    ihub_initiate_vend_succes = matched[
        (matched["VENDOR_STATUS"].str.lower() == "success")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "initiated")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
    ].copy()
    ihub_initiate_vend_succes["CATEGORY"] = "IHUB_INT_VEND_SUC"
    ihub_initiate_vend_succes = safe_column_select(
        ihub_initiate_vend_succes, required_columns
    )

    # SCENARIO 6 VEND_FAIL_IHUB_INT IL
    ihub_initiate_vend_fail = matched[
        (matched["VENDOR_STATUS"].str.lower() == "failed")
        & (matched["IHUB_MASTER_STATUS"].str.lower() == "initiated")
        & (matched["IHUB_LEDGER_STATUS"].str.lower() == "yes")
    ].copy()
    ihub_initiate_vend_fail["CATEGORY"] = "VEND_FAIL_IHUB_INT"
    ihub_initiate_vend_fail = safe_column_select(
        ihub_initiate_vend_fail, required_columns
    )
    # Scenario Block ends-----------------------------------------------------------------------------------

    # Old lines of codes--------------------------------------------------------------------------------
    # # 6. VENDOR_SUCCESS_IHUB_INITIATED
    # vendor_success_ihub_initiated = mismatched[
    #     (mismatched["VENDOR_STATUS"].str.lower() == "success")
    #     & (mismatched["IHUB_MASTER_STATUS"].str.lower() == "initiated")
    # ].copy()

    # vendor_success_ihub_initiated["CATEGORY"] = "VENDOR_SUCCESS_IHUB_INITIATED"
    # vendor_success_ihub_initiated = safe_column_select(
    #     vendor_success_ihub_initiated, required_columns
    # )

    # # 7. VENDOR_SUCCESS_IHUB_FAILED
    # vendor_success_ihub_failed = mismatched[
    #     (mismatched["VENDOR_STATUS"].str.lower() == "success")
    #     & (mismatched["IHUB_MASTER_STATUS"].str.lower() == "failed")
    # ].copy()

    # vendor_success_ihub_failed["CATEGORY"] = "VENDOR_SUCCESS_IHUB_FAILED"
    # vendor_success_ihub_failed = safe_column_select(
    #     vendor_success_ihub_failed, required_columns
    # )

    # # 8. VENDOR_FAILED_IHUB_INITIATED
    # vendor_failed_ihub_initiated = mismatched[
    #     (mismatched["VENDOR_STATUS"].str.lower() == "failed")
    #     & (mismatched["IHUB_MASTER_STATUS"].str.lower() == "initiated")
    # ].copy()

    # vendor_failed_ihub_initiated["CATEGORY"] = "VENDOR_FAILED_IHUB_INITIATED"
    # vendor_failed_ihub_initiated = safe_column_select(
    #     vendor_failed_ihub_initiated, required_columns
    # )
    # # 9.
    # vend_ihub_succes_not_in_ledger = matched[
    #     (matched["VENDOR_STATUS"].str.lower() == "success")
    #     & (matched["IHUB_MASTER_STATUS"].str.lower() == "success")
    #     & (matched["IHUB_LEDGER_STATUS"].str.lower() == "no")
    # ].copy()

    # vend_ihub_succes_not_in_ledger["CATEGORY"] = "VENDOR & IHUB SUCCESS_NOTIN_LEDGER"
    # vend_ihub_succes_not_in_ledger = safe_column_select(
    #     vend_ihub_succes_not_in_ledger, required_columns
    # )Ends---------------------------------------------------------------------------------------

    tenant_data["CATEGORY"] = "TENANT_DB_INTI - NOT_IN_IHUB"

    # Combining all Scenarios
    combined = [
        not_in_vendor,
        not_in_portal,
        # not_in_portal_vendor_success,
        # mismatched,
        tenant_data,
        vend_ihub_succ_not_in_ledger,
        vend_fail_ihub_succ_not_in_ledger,
        vend_succ_ihub_fail_not_in_ledger,
        # ihub_vend_fail_not_in_ledger,
        ihub_initiate_vend_succes_not_in_ledger,
        ihub_initiate_vend_fail_not_in_ledger,
        # vend_ihub_succ,
        vend_fail_ihub_succ,
        vend_succ_ihub_fail,
        # ihub_vend_fail,
        ihub_initiate_vend_succes,
        ihub_initiate_vend_fail,
    ]
    all_columns = set().union(*[df.columns for df in combined])
    aligned_dfs = []
    for df in combined:
        # create missing columns with None
        df_copy = df.copy()  # 🛡️ Make a copy so original is not modified
        for col in all_columns - set(df_copy.columns):
            df_copy[col] = None
        df_copy = df_copy[list(all_columns)]  # Reorder columns
        aligned_dfs.append(df_copy)
    # Filter out DataFrames that are completely empty or contain only NA values
    non_empty_dfs = [
        df for df in aligned_dfs if not df.empty and not df.isna().all().all()
    ]
    combined = pd.concat(non_empty_dfs, ignore_index=True)
    logger.info("Filteration Ends")

    # Mapping all Scenarios with keys as Dictionary to retrun as result
    mapping = {
        "not_in_vendor": not_in_vendor,
        "combined": combined,
        "not_in_Portal": not_in_portal,
        # "mismatched": mismatched,
        # "NOT_IN_PORTAL_VENDOR_SUCC": not_in_portal_vendor_success,
        "Tenant_db_ini_not_in_hubdb": tenant_data,
        "VEND_IHUB_SUC-NIL": vend_ihub_succ_not_in_ledger,
        "VEND_FAIL_IHUB_SUC-NIL": vend_fail_ihub_succ_not_in_ledger,
        "VEND_SUC_IHUB_FAIL-NIL": vend_succ_ihub_fail_not_in_ledger,
        # "IHUB_VEND_FAIL-NIL": ihub_vend_fail_not_in_ledger,
        "IHUB_INT_VEND_SUC-NIL": ihub_initiate_vend_succes_not_in_ledger,
        "VEND_FAIL_IHUB_INT-NIL": ihub_initiate_vend_fail_not_in_ledger,
        # "VEND_IHUB_SUC": vend_ihub_succ,
        "VEND_FAIL_IHUB_SUC": vend_fail_ihub_succ,
        "VEND_SUC_IHUB_FAIL": vend_succ_ihub_fail,
        # "IHUB_VEND_FAIL": ihub_vend_fail,
        "IHUB_INT_VEND_SUC": ihub_initiate_vend_succes,
        "VEND_FAIL_IHUB_INT": ihub_initiate_vend_fail,
        "Total_Success_count": success_count,
        "Total_Failed_count": failed_count,
    }

    return mapping


# Filteration Function Ends-------------------------------------------------------------------


# Ebo Wallet Amount and commission  Debit credit check function  -------------------------------------------
def get_ebo_wallet_data(start_date, end_date):
    logger.info("Fetching Data from EBO Wallet Transaction")
    ebo_df = None
    query = text(
        f"""
    SELECT  
        mt2.TransactionRefNum,
        ewt.MasterTransactionsId,
        MAX(CASE WHEN ewt.Description = 'Transaction - Credit' THEN 'Yes' ELSE 'No' END) AS TRANSACTION_CREDIT,
        MAX(CASE WHEN ewt.Description = 'Transaction - Debit' THEN 'Yes' ELSE 'No' END) AS TRANSACTION_DEBIT,
        MAX(CASE WHEN ewt.Description = 'Commission Added' THEN 'Yes' ELSE 'No' END) AS COMMISSION_CREDIT,
        MAX(CASE WHEN ewt.Description = 'Commission - Reversal' THEN 'Yes' ELSE 'No' END) AS COMMISSION_REVERSAL
    FROM
        ihubcore.MasterTransaction mt2
    JOIN  
        tenantinetcsc.EboWalletTransaction ewt
        ON mt2.TenantMasterTransactionId = ewt.MasterTransactionsId
    WHERE
        DATE(mt2.CreationTs) BETWEEN :start_date AND :end_date
    GROUP BY
        mt2.TransactionRefNum,
        ewt.MasterTransactionsId
    """
    )
    try:
        with engine.connect().execution_options(command_timeout=60) as connection:
            # Execute with retry logic
            ebo_df = execute_sql_with_retry(
                connection,
                query,
                params={"start_date": start_date, "end_date": end_date},
            )
            if ebo_df.empty:
                logger.warning(f"No data returned from Ebo Wallet table")

    except SQLAlchemyError as e:
        logger.error(f"Database error in recharge_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in Ebo Wallet Query Execution: {e}")
    return ebo_df


# ----------------------------------------------------------------------------------


# tenant database filtering function------------------------------------------------
def tenant_filtering(
    start_date,
    end_date,
    tenant_service_id,
    Hub_service_id,
):
    logger.info("Entered Tenant filtering function")
    result = None
    # To find transaction that is initiated by EBO present in tenant data base But do not hit in hub database
    query = f"""
         WITH cte AS (
         SELECT src.Id as TENANT_DB_Id, src.UserName as IHUB_USERNAME, src.TranAmountTotal as AMOUNT,src.TransactionStatus as TENANT_STATUS,
         src.CreationTs as SERVICE_DATE, src.VendorSubServiceMappingId,hub.Id AS hub_id
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
         AND DATE(hub.CreationTs) BETWEEN '{start_date}' AND '{end_date}'
         AND hub.VendorSubServiceMappingId in ({Hub_service_id})
         )
         SELECT *
         FROM cte
         WHERE hub_id IS NULL"""
    try:
        with engine.connect().execution_options(command_timeout=60) as connection:
            # result has the record for data that trigerred in tenant but not hit hub
            result = execute_sql_with_retry(
                connection,
                query,
            )

    except SQLAlchemyError as e:
        logger.error(f"Database error in Tenant DB Filtering: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in Tenant DB Filtering: {e}")
    return result


# -----------------------------------------------------------------------------


# Recharge service function ---------------------------------------------------
def recharge_Service(start_date, end_date, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    result = pd.DataFrame()  # Initialize as empty DataFrame

    # Use parameterized query to prevent SQL injection
    query = text(
        f"""
        SELECT mt2.TransactionRefNum AS IHUB_REFERENCE,
               mt2.TenantDetailId as TENANT_ID,   
               sn.requestID AS VENDOR_REFERENCE,
               u.UserName as IHUB_USERNAME, 
               mt2.TransactionStatus AS IHUB_MASTER_STATUS,
               sn.CreationTs AS SERVICE_DATE, 
               sn.rechargeStatus AS {service_name}_STATUS,
               CASE
                   WHEN iwt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS IHUB_LEDGER_STATUS,
               CASE
                   WHEN twt.IHubReferenceId IS NOT NULL THEN 'Yes'
                   ELSE 'No'
               END AS TENANT_LEDGER_STATUS
        FROM ihubcore.MasterTransaction mt2
        LEFT JOIN ihubcore.MasterSubTransaction mst ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN ihubcore.PsRechargeTransaction sn ON sn.MasterSubTransactionId = mst.Id
        LEFT JOIN tenantinetcsc.EboDetail ed ON mt2.EboDetailId = ed.Id
        LEFT JOIN tenantinetcsc.`User` u ON u.Id = ed.UserId
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.IHubWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND CURRENT_DATE()
        ) iwt ON iwt.IHubReferenceId = mt2.TransactionRefNum
        LEFT JOIN (
            SELECT DISTINCT IHubReferenceId
            FROM ihubcore.TenantWalletTransaction
            WHERE DATE(CreationTs) BETWEEN :start_date AND CURRENT_DATE()
        ) twt ON twt.IHubReferenceId = mt2.TransactionRefNum
        WHERE DATE(sn.CreationTs) BETWEEN :start_date AND :end_date
    """
    )

    try:
        with engine.connect().execution_options(command_timeout=60) as connection:
            # Execute with retry logic
            df_db = execute_sql_with_retry(
                connection,
                query,
                params={"start_date": start_date, "end_date": end_date},
            )

            if df_db.empty:
                logger.warning(f"No data returned for service: {service_name}")
                return pd.DataFrame()

            # Status mapping with fallback
            status_mapping = {
                0: "initiated",
                1: "success",
                2: "pending",
                3: "failed",
                4: "instant failed",
            }
            df_db[f"{service_name}_STATUS"] = (
                df_db[f"{service_name}_STATUS"]
                .map(status_mapping)
                .fillna(df_db[f"{service_name}_STATUS"])
            )
            tenant_Id_mapping = {
                1: "INET-CSC",
                2: "ITI-ESEVA",
                3: "UPCB",
            }
            df_db["TENANT_ID"] = (
                df_db["TENANT_ID"].map(tenant_Id_mapping).fillna(df_db["TENANT_ID"])
            )

            # Get wallet data with retry
            ebo_result = get_ebo_wallet_data(start_date, end_date)

            if ebo_result is not None and not ebo_result.empty:
                result = pd.merge(
                    df_db,
                    ebo_result,
                    how="left",
                    left_on="IHUB_REFERENCE",
                    right_on="TransactionRefNum",
                    validate="one_to_one",  # Add validation
                )
            else:
                logger.warning("No ebo wallet data returned")
                result = df_db

    except SQLAlchemyError as e:
        logger.error(f"Database error in recharge_Service(): {e}")
    except Exception as e:
        logger.error(f"Unexpected error in recharge_Service(): {e}")

    return result


# ---------------------------------------------------------------------------------------


# IMT SERVICE FUNCTION-------------------------------------------------------------------
def IMT_Service(start_date, end_date, df_excel, service_name):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f"""
            SELECT mt2.TransactionRefNum AS IHUB_REFERENCE,
            pst.VendorReferenceId as VENDOR_REFERENCE,
            u.UserName as IHUB_USERNAME,
            mt2.TransactionStatus AS IHUB_MASTER_STATUS,
            pst.PaySprintTransStatus as {service_name}_STATUS,
            CASE
            WHEN a.IHubReferenceId  IS NOT NULL THEN 'Yes'
            ELSE 'No'
            END AS IHUB_LEDGER_STATUS
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
    df_db["VENDOR_REFERENCE"] = df_db["VENDOR_REFERENCE"].astype(str)
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
        END AS IHUB_REFUND_STATUS
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
        left_on="VENDOR_REFERENCE",
        right_on="VendorReferenceId",
    )
    merged_df.drop(columns=["VendorReferenceId"], inplace=True)
    df_db = merged_df
    df_db["IHUB_REFUND_STATUS"] = df_db["IHUB_REFUND_STATUS"].fillna("not_applicable")
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
    df_db[f"{service_name}_STATUS"] = df_db[f"{service_name}_STATUS"].apply(
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
        mt2.TransactionRefNum as IHUB_REFERENCE,
        u.Username as IHUB_USERNAME,
        bbp.TxnRefId  as VENDOR_REFERENCE,
        mt2.TransactionStatus AS IHUB_MASTER_STATUS,
        bbp.TransactionStatusType,bbp.HeadReferenceId ,
        CASE when iw.IHubReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS IHUB_LEDGER_STATUS,
        CASE when bf.HeadReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS BILL_FETCH_STATUS
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
    df_db[f"{service_name}_STATUS"] = df_db[f"{service_name}_STATUS"].apply(
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
        select u.ApplicationNumber as VENDOR_REFERENCE,
        u.UTITSLTransID_Gateway as UTITSLTrans_id,
        mt.TransactionRefNum AS IHUB_REFERENCE,
        mt.TransactionStatus AS IHUB_MASTER_STATUS,
        u.TransactionStatusType as {service_name}_STATUS,
        CASE When 
        a.IHubReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS 'IHUB_LEDGER_STATUS'
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
    df_db[f"{service_name}_STATUS"] = df_db[f"{service_name}_STATUS"].apply(
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
        select pit.AcknowledgeNo as VENDOR_REFERENCE,mt.TransactionRefNum AS IHUB_REFERENCE,
        mt.TransactionStatus AS IHUB_MASTER_STATUS,
        u.Username as IHUB_USERNAME,
        pit.applicationstatus as {service_name}_STATUS,
        CASE When 
        a.IHubReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'NO'
        END AS 'IHUB_LEDGER_STATUS'
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
    df_db[f"{service_name}_STATUS"] = df_db[f"{service_name}_STATUS"].apply(
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
