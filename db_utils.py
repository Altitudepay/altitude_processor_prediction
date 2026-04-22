
# db_utils.py
import os

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

errors = [
    "Stolen card, pick up",
    "Expired card",
    "Insufficient card funds",
    "Blocked credit card. Contact the issuer before trying again.",
    "Card expired",
    "Card is blocked",
    "Card Mask Blacklisted: Card '430589******1006'",
    "Disabled card",
    "Invalid card expiry date",
    "Invalid card number",
    "Invalid credentials",
    "Lost Card",
    "No card record",
    "Restricted Card",
    "Transaction failed: Invalid card number",
    "Value '416598xxxxxx1534' is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship",
    "Value '462239xxxxxx7713' is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship",
    "Insufficient funds",
    "Over credit limit",
    "Card reported lost",
    "Card reported stolen",
    "Pick up card",
    "Card not active",
    "Card not yet effective",
    "Invalid card status",
    "Account closed",
    "Card suspended",
    "Invalid CVV / CVC",
    "Invalid PAN",
    "Invalid PIN",
    "Invalid card data",
    "Invalid card verification value",
    "Incorrect PIN",
    "Invalid card credentials",
    "Card not recognized",
]


def _get_connection():
    return psycopg2.connect(
        host=st.secrets.get("DB_HOST"),
        port=st.secrets.get("DB_PORT", 5432),
        dbname=st.secrets.get("DB_NAME"),
        user=st.secrets.get("DB_USER"),
        password=st.secrets.get("DB_PASSWORD"),
    )


def run_bin_query():
    query = """
        WITH filtered_txn AS (
            SELECT txid, error_description
            FROM altitude_transaction
            WHERE created_date >= date_trunc('month', CURRENT_DATE - INTERVAL '1 month')
              AND created_date < date_trunc('month', CURRENT_DATE)
        )
        SELECT DISTINCT LEFT(c.card_no, 6) AS bin
        FROM filtered_txn t
        JOIN altitude_customers c ON c.txid = t.txid
        WHERE NOT EXISTS (
            SELECT 1
            FROM error_blacklist e
            WHERE e.error_description = t.error_description
        );
    """
    try:
        conn = _get_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df["bin"].tolist()
    except Exception:
        return []


def run_processor_query():
    query = """
        SELECT DISTINCT t.processor_name
        FROM public.altitude_transaction t
        LEFT JOIN public.error_blacklist e
            ON e.error_description = t.error_description
        WHERE t.created_date >= date_trunc('month', CURRENT_DATE - INTERVAL '1 month')
          AND t.created_date < date_trunc('month', CURRENT_DATE)
          AND e.error_description IS NULL;
    """
    try:
        conn = _get_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df["processor_name"].tolist()
    except Exception:
        return []


def fetch_bin_processor_stats():
    query = """
        WITH base AS (
            -- Single scan of the view with date filter applied once
            SELECT
                t.txid,
                t.processor_name,
                t.status,
                t.error_description,
                t.created_datetime
            FROM public.vw_altitude_transaction_master t
            WHERE t.created_date >= CURRENT_DATE - INTERVAL '1 month'
        ),

        first_tx_times AS (
            -- Get the minimum datetime per txid (same as original)
            SELECT
                txid,
                MIN(created_datetime) AS first_transaction_datetime
            FROM base
            GROUP BY txid
        ),

        first_tx AS (
            -- Join back to get ALL rows at that min timestamp (preserves ties like original)
            -- Apply blacklist filter here on the already date-filtered base
            SELECT
                b.txid,
                b.processor_name,
                b.status
            FROM base b
            INNER JOIN first_tx_times ftt
                ON  b.txid = ftt.txid
                AND b.created_datetime = ftt.first_transaction_datetime
            WHERE NOT EXISTS (
                SELECT 1
                FROM public.error_blacklist e
                WHERE e.error_description = b.error_description
            )
        ),

        tarns AS (
            SELECT
                LEFT(c.card_no, 6) AS bin,
                f.processor_name,
                f.status
            FROM first_tx f
            INNER JOIN public.altitude_customers c ON c.txid = f.txid
        )

        SELECT
            t.bin,
            t.processor_name,
            COUNT(*) FILTER (WHERE t.status IN ('approved', 'declined'))  AS total,
            COUNT(*) FILTER (WHERE t.status = 'approved')                 AS total_success,
            ROUND(
                COUNT(*) FILTER (WHERE t.status = 'approved')::numeric
                / NULLIF(COUNT(*) FILTER (WHERE t.status IN ('approved', 'declined')), 0) * 100,
                4
            ) AS approval_rate
        FROM tarns t
        GROUP BY t.bin, t.processor_name
        ORDER BY t.bin DESC;
    """
    try:
        conn = _get_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception:
        return []


def fetch_bin_processor_ar(start_date, end_date, bin_list, processor_list):
    bin_values = ", ".join(f"'{b}'" for b in bin_list)
    query = f"""
        WITH blacklist_filtered AS (
            SELECT
                at.txid,
                at.processor_name,
                at.status,
                at.created_datetime
            FROM altitude_transaction at
            WHERE at.created_date BETWEEN '2026-01-01' AND '2026-01-31'
              AND at.error_description NOT IN (
                  'Stolen card, pick up',
                  'Expired card',
                  'Insufficient card funds',
                  'Blocked credit card. Contact the issuer before trying again.',
                  'Card expired',
                  'Card is blocked',
                  'Card Mask Blacklisted: Card ''430589******1006''',
                  'Disabled card',
                  'Invalid card expiry date',
                  'Invalid card number',
                  'Invalid credentials',
                  'Lost Card',
                  'No card record',
                  'Restricted Card',
                  'Transaction failed: Invalid card number',
                  'Value ''416598xxxxxx1534'' is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship',
                  'Value ''462239xxxxxx7713'' is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship',
                  'Insufficient funds',
                  'Over credit limit',
                  'Card reported lost',
                  'Card reported stolen',
                  'Pick up card',
                  'Card not active',
                  'Card not yet effective',
                  'Invalid card status',
                  'Account closed',
                  'Card suspended',
                  'Invalid CVV / CVC',
                  'Invalid PAN',
                  'Invalid PIN',
                  'Invalid card data',
                  'Invalid card verification value',
                  'Incorrect PIN',
                  'Invalid card credentials',
                  'Card not recognized'
              )
        ),

        first_tx_times AS (
            SELECT
                txid,
                MIN(created_datetime) AS first_transaction_datetime
            FROM blacklist_filtered
            GROUP BY txid
        ),

        first_transaction AS (
            SELECT
                txid,
                processor_name,
                status
            FROM (
                SELECT
                    bf.txid,
                    bf.processor_name,
                    bf.status,
                    ROW_NUMBER() OVER (
                        PARTITION BY bf.txid, bf.processor_name
                        ORDER BY bf.created_datetime
                    ) AS rn
                FROM blacklist_filtered bf
                INNER JOIN first_tx_times ftt
                    ON bf.txid = ftt.txid
                    AND bf.created_datetime = ftt.first_transaction_datetime
            ) ranked
            WHERE rn = 1
        ),

        transaction_with_card AS (
            SELECT
                ft.processor_name,
                ft.status,
                LEFT(c.card_no, 6) AS bin
            FROM first_transaction ft
            INNER JOIN altitude_customers c ON c.txid = ft.txid
            WHERE LEFT(c.card_no, 6) IN (
                '222300',
                '999999'
                -- add more BINs here, one per line
            )
        ),

        aggregated_result AS (
            SELECT
                bin,
                processor_name AS processor,
                COUNT(*) FILTER (WHERE status IN ('approved', 'declined')) AS total,
                COUNT(*) FILTER (WHERE status = 'approved')           AS total_success
            FROM transaction_with_card
            GROUP BY bin, processor_name
        )

        SELECT
            bin,
            processor,
            total,
            total_success,
            ROUND(
                ((total_success::FLOAT / NULLIF(total, 0)::FLOAT) * 100)::numeric,
                3
            ) AS ar,
            DENSE_RANK() OVER (
                PARTITION BY bin
                ORDER BY ROUND(
                    ((total_success::FLOAT / NULLIF(total, 0)::FLOAT) * 100)::numeric,
                    3
                ) DESC
            ) AS rnk
        FROM aggregated_result
        ORDER BY bin, rnk;
    """
    try:
        conn = _get_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(e)
        return []

# # 
# # db_utils.py
# import pandas as pd
# import psycopg2
# from dotenv import load_dotenv
# import os
# import streamlit as st

# load_dotenv()
# errors = """'Stolen card, pick up','Expired card','Insufficient card funds','Blocked credit card. Contact the issuer before trying again.','Card expired','Card is blocked','Card Mask Blacklisted: Card ‘430589******1006’','Disabled card','Invalid card expiry date','Invalid card number','Invalid credentials','Lost Card','No card record','Restricted Card','Transaction failed: Invalid card number','Value ‘416598xxxxxx1534’ is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship','Value ‘462239xxxxxx7713’ is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship','Insufficient funds','Over credit limit','Card reported lost','Card reported stolen','Pick up card','Card not active','Card not yet effective','Invalid card status','Account closed','Card suspended','Invalid CVV / CVC','Invalid PAN','Invalid PIN','Invalid card data','Invalid card verification value','Incorrect PIN','Invalid card credentials','Card not recognized'"""

# def run_bin_query():
#     query = f"""
#         SELECT DISTINCT LEFT(card_no, 6) AS bin
#         FROM public.altitude_transaction t
#         LEFT JOIN public.altitude_project p ON t.project_id = p.project_id
#         LEFT JOIN public.altitude_customers c ON t.txid = c.txid
#         WHERE EXTRACT(YEAR FROM t.created_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')
#           AND EXTRACT(MONTH FROM t.created_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
#         AND t.error_description NOT IN ({errors})
#     """
#     try:
#         conn = psycopg2.connect(
#             host=st.secrets.get("DB_HOST"),
#             port=st.secrets.get("DB_PORT", 5432),
#             dbname=st.secrets.get("DB_NAME"),
#             user=st.secrets.get("DB_USER"),
#             password=st.secrets.get("DB_PASSWORD")
#         )
#         df = pd.read_sql_query(query, conn)
#         conn.close()
#         bin_list = df['bin'].tolist()
#         return bin_list
#     except Exception as e:
#         return []

# def run_processor_query():
#     query = f"""
#         SELECT DISTINCT t.processor_name
#         FROM public.altitude_transaction t
#         WHERE EXTRACT(YEAR FROM t.created_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')
#           AND EXTRACT(MONTH FROM t.created_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
#           AND t.error_description NOT IN ({errors})
#     """
#     try:
#         conn = psycopg2.connect(
#             host=st.secrets.get("DB_HOST"),
#             port=st.secrets.get("DB_PORT", 5432),
#             dbname=st.secrets.get("DB_NAME"),
#             user=st.secrets.get("DB_USER"),
#             password=st.secrets.get("DB_PASSWORD")
#         )
#         df = pd.read_sql_query(query, conn)
#         conn.close()
#         bin_list = df['processor_name'].tolist()
#         return bin_list
#     except Exception as e:
#         return []


# def fetch_bin_processor_stats():
#     try:
#         conn = psycopg2.connect(
#             host=st.secrets.get("DB_HOST"),
#             port=st.secrets.get("DB_PORT", 5432),
#             dbname=st.secrets.get("DB_NAME"),
#             user=st.secrets.get("DB_USER"),
#             password=st.secrets.get("DB_PASSWORD")
#         )
#         query = f"""
#             WITH first_tx_times AS (
#     -- Step 1: Find the exact first transaction time for each txid within the last month.
#     -- This uses vw_altitude_transaction_master to allow for filtering on error_description later.
# 		    SELECT
# 		        txid,
# 		        MIN(created_datetime) AS first_transaction_datetime
# 		    FROM public.vw_altitude_transaction_master
# 		    WHERE created_date >= CURRENT_DATE - INTERVAL '1 month'
# 		    GROUP BY txid
# 		),
# 		tarns AS (
# 		    -- Step 2: Get the details of that first transaction, joining to get the card BIN.
# 		    -- Crucially, this step filters out transactions with specific, non-retryable card/issuer errors.
# 		    SELECT
# 		        LEFT(c.card_no, 6) AS bin,
# 		        t.processor_name,
# 		        t.status
# 		    FROM public.vw_altitude_transaction_master t
# 		    INNER JOIN public.altitude_customers c ON t.txid = c.txid
# 		    INNER JOIN first_tx_times ftt ON t.txid = ftt.txid AND t.created_datetime = ftt.first_transaction_datetime
# 		    -- This is the new logic: excluding transactions that failed due to terminal card issues.
# 		    WHERE t.error_description NOT IN (
# 		        'Stolen card, pick up','Expired card','Insufficient card funds','Blocked credit card. Contact the issuer before trying again.','Card expired',
# 		        'Card is blocked','Card Mask Blacklisted: Card ‘430589******1006’','Disabled card','Invalid card expiry date','Invalid card number',
# 		        'Invalid credentials','Lost Card','No card record','Restricted Card','Transaction failed: Invalid card number',
# 		        'Value ‘416598xxxxxx1534’ is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship',
# 		        'Value ‘462239xxxxxx7713’ is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship',
# 		        'Insufficient funds','Over credit limit','Card reported lost','Card reported stolen','Pick up card','Card not active',
# 		        'Card not yet effective','Invalid card status','Account closed','Card suspended','Invalid CVV / CVC','Invalid PAN','Invalid PIN',
# 		        'Invalid card data','Invalid card verification value','Incorrect PIN','Invalid card credentials','Card not recognized'
# 		    )
# 		),
# 		summary AS (
# 		    -- Step 3: Aggregate the filtered results to count total and successful transactions.
# 		    -- This uses the more efficient FILTER clause instead of two separate CTEs.
# 		    SELECT
# 		        bin,
# 		        processor_name,
# 		        COUNT(*) FILTER (WHERE status in ('approved','declined')) AS total,
# 		        COUNT(*) FILTER (WHERE status = 'approved') AS total_success
# 		    FROM tarns
# 		    GROUP BY bin, processor_name
# 		)
# 		-- Final Step: Calculate the approval rate and present the final columns.
# 		SELECT
# 		    s.bin,
# 		    s.processor_name,
# 		    s.total,
# 		    s.total_success,
# 		    ROUND(
# 		        s.total_success::numeric / NULLIF(s.total, 0) * 100, 4
# 		    ) AS approval_rate
# 		FROM
# 		    summary s
# 		ORDER BY s.bin DESC;
#         """
#         df = pd.read_sql(query, conn)
#         conn.close()
#         return df
#     except Exception as e:
#         return []
# def fetch_bin_processor_ar(start_date, end_date,bin_list,processor_list):
#     try:
#         conn = psycopg2.connect(
#             host=st.secrets.get("DB_HOST"),
#             port=st.secrets.get("DB_PORT", 5432),
#             dbname=st.secrets.get("DB_NAME"),
#             user=st.secrets.get("DB_USER"),
#             password=st.secrets.get("DB_PASSWORD")
#         )
#         query = f"""
#            WITH first_tx_times AS (
#                 SELECT
#                     txid,
#                     MIN(created_datetime) AS first_transaction_datetime
#                 FROM altitude_transaction
#                 WHERE created_date BETWEEN '{start_date}' AND '{end_date}'
#                 AND error_description NOT IN ({errors})
#                 GROUP BY txid
#             ),
#             first_transaction AS (
#                 SELECT DISTINCT ON (at.txid, at.processor_name)
#                     at.txid,
#                     at.processor_name,
#                     at.status,
#                     at.created_datetime AS first_transaction_time
#                 FROM altitude_transaction at
#                 INNER JOIN first_tx_times ftt
#                     ON at.txid = ftt.txid
#                     AND at.created_datetime = ftt.first_transaction_datetime
#                 ORDER BY at.txid, at.processor_name, at.created_datetime
#             ),
#             transaction_with_card AS (
#                 SELECT
#                     at.txid,
#                     at.processor_name,
#                     at.status,
#                     LEFT(c.card_no, 6) AS BIN
#                 FROM altitude_transaction at
#                 INNER JOIN first_transaction ft
#                     ON at.txid = ft.txid
#                     AND at.processor_name = ft.processor_name
#                     AND at.created_datetime = ft.first_transaction_time
#                 INNER JOIN altitude_customers c
#                     ON at.txid = c.txid
#                 WHERE LEFT(c.card_no, 6) IN ({','.join(f"'{bin}'" for bin in bin_list)})
#             ),
#             aggregated_result AS (
#                 SELECT
#                     BIN,
#                     processor_name AS Processor,
#                     COUNT(*) FILTER (WHERE status in ('approved','declined')) AS Total,
#                     COUNT(*) FILTER (WHERE status = 'approved') AS Total_Success
#                 FROM transaction_with_card
#                 GROUP BY BIN, processor_name
#             )
#             SELECT
#                 BIN,
#                 Processor,
#                 Total,
# 		Total_Success,
#                 ROUND(
# 		    ((Total_Success::FLOAT / NULLIF(Total, 0)::FLOAT) * 100)::numeric,
# 		    3
# 		) AS Ar
#   		,
# 		DENSE_RANK() OVER(
# 				Partition by BIN ORDER BY ROUND(
# 		    	((Total_Success::FLOAT / NULLIF(Total, 0)::FLOAT) * 100)::numeric,
# 				    3
# 				) DESC
# 				) AS Rnk
#             FROM aggregated_result;
#         """
#         df = pd.read_sql(query, conn)
#         conn.close()
# 	# print(df)
#         return df
#     except Exception as e:
# 	    print(e)
# 	# print(e)
#         # return []
