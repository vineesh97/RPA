import pandas as pd
from db_connector import get_db_connection
from logger_config import logger

engine = get_db_connection()


def inward_service_selection(
    start_date, end_date, service_name, transaction_type, df_excel
):
    if service_name == "Aeps":
        df_excel = df_excel.rename(columns={"UTR": "REFID", "DATE": "VEND_DATE"})
        logger.info("Aeps service: Column 'UTR' renamed to 'REFID'")
        result = aeps_Service(
            start_date, end_date, service_name, transaction_type, df_excel
        )
    return result


def filtering_Data(df_db, df_excel, service_name):
    logger.info("Filteration Starts")
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
    ].copy()

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
    }


# Aeps function
def aeps_Service(start_date, end_date, service_name, transaction_type, df_excel):
    logger.info(f"Fetching data from HUB for {service_name}")
    query = f"""
        SELECT 
        mt2.TransactionRefNum AS Ihub_reference,
        pat.BankRrn AS vendor_reference,
        u.UserName,
        mt2.TransactionStatus AS IHUB_Master_status,
        mst.TransactionStatus AS MasterSubTrans_status,
        pat.CreationTs AS service_date,
        pat.TransStatus AS {service_name}_status,
        CASE 
        WHEN a.IHubReferenceId IS NOT NULL THEN 'Yes'
        ELSE 'No'
        END AS Ihub_Ledger_status
        FROM ihubcore.MasterTransaction mt2 
        LEFT JOIN ihubcore.MasterSubTransaction mst
        ON mst.MasterTransactionId = mt2.Id
        LEFT JOIN ihubcore.PsAepsTransaction pat 
        ON pat.MasterSubTransactionId = mst.Id
        LEFT JOIN tenantinetcsc.EboDetail ed
        ON mt2.EboDetailId = ed.Id
        LEFT JOIN tenantinetcsc.`User` u
        ON u.id = ed.UserId
        LEFT JOIN (
        SELECT DISTINCT iwt.IHubReferenceId AS IHubReferenceId
        FROM ihubcore.IHubWalletTransaction iwt
        WHERE DATE(iwt.CreationTs) BETWEEN '{start_date}' AND CURRENT_DATE()
        ) a 
        ON a.IHubReferenceId = mt2.TransactionRefNum
        WHERE mt2.TenantDetailId = 1 and pat.TransMode={transaction_type}
        AND DATE(pat.CreationTs) BETWEEN '{start_date}' AND '{end_date}';
    """
    df_db = pd.read_sql(query, con=engine)
    # mapping status name with enum
    status_mapping = {
        3: "inprocess",
        2: "timeout",
        1: "success",
        255: "initiated",
        254: "failed",
        0: "failed",
    }
    df_db[f"{service_name}_status"] = df_db[f"{service_name}_status"].apply(
        lambda x: status_mapping.get(x, x)
    )
    # calling filtering function
    result = filtering_Data(df_db, df_excel, service_name)
    return result
