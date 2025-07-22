# 
# db_utils.py
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
errors = """'Stolen card, pick up','Expired card','Insufficient card funds','Blocked credit card. Contact the issuer before trying again.','Card expired','Card is blocked','Card Mask Blacklisted: Card ‘430589******1006’','Disabled card','Invalid card expiry date','Invalid card number','Invalid credentials','Lost Card','No card record','Restricted Card','Transaction failed: Invalid card number','Value ‘416598xxxxxx1534’ is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship','Value ‘462239xxxxxx7713’ is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship','Insufficient funds','Over credit limit','Card reported lost','Card reported stolen','Pick up card','Card not active','Card not yet effective','Invalid card status','Account closed','Card suspended','Invalid CVV / CVC','Invalid PAN','Invalid PIN','Invalid card data','Invalid card verification value','Incorrect PIN','Invalid card credentials','Card not recognized'"""

def run_bin_query():
    query = f"""
        SELECT DISTINCT LEFT(card_no, 6) AS bin
        FROM public.altitude_transaction t
        LEFT JOIN public.altitude_project p ON t.project_id = p.project_id
        LEFT JOIN public.altitude_customers c ON t.txid = c.txid
        WHERE EXTRACT(YEAR FROM t.created_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')
          AND EXTRACT(MONTH FROM t.created_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
        AND t.error_description NOT IN ({errors})
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        df = pd.read_sql_query(query, conn)
        conn.close()
        bin_list = df['bin'].tolist()
        return bin_list
    except Exception as e:
        return []

def run_processor_query():
    query = f"""
        SELECT DISTINCT t.processor_name
        FROM public.altitude_transaction t
        WHERE EXTRACT(YEAR FROM t.created_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')
          AND EXTRACT(MONTH FROM t.created_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
          AND t.error_description NOT IN ({errors})
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        df = pd.read_sql_query(query, conn)
        conn.close()
        bin_list = df['processor_name'].tolist()
        return bin_list
    except Exception as e:
        return []


def fetch_bin_processor_stats():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        query = f"""
            WITH first_tx_times AS (
    -- Step 1: Find the exact first transaction time for each txid within the last month.
    -- This uses vw_altitude_transaction_master to allow for filtering on error_description later.
		    SELECT
		        txid,
		        MIN(created_datetime) AS first_transaction_datetime
		    FROM public.vw_altitude_transaction_master
		    WHERE created_date >= CURRENT_DATE - INTERVAL '1 month'
		    GROUP BY txid
		),
		tarns AS (
		    -- Step 2: Get the details of that first transaction, joining to get the card BIN.
		    -- Crucially, this step filters out transactions with specific, non-retryable card/issuer errors.
		    SELECT
		        LEFT(c.card_no, 6) AS bin,
		        t.processor_name,
		        t.status
		    FROM public.vw_altitude_transaction_master t
		    INNER JOIN public.altitude_customers c ON t.txid = c.txid
		    INNER JOIN first_tx_times ftt ON t.txid = ftt.txid AND t.created_datetime = ftt.first_transaction_datetime
		    -- This is the new logic: excluding transactions that failed due to terminal card issues.
		    WHERE t.error_description NOT IN (
		        'Stolen card, pick up','Expired card','Insufficient card funds','Blocked credit card. Contact the issuer before trying again.','Card expired',
		        'Card is blocked','Card Mask Blacklisted: Card ‘430589******1006’','Disabled card','Invalid card expiry date','Invalid card number',
		        'Invalid credentials','Lost Card','No card record','Restricted Card','Transaction failed: Invalid card number',
		        'Value ‘416598xxxxxx1534’ is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship',
		        'Value ‘462239xxxxxx7713’ is invalid. The combination of currency, card type and transaction type is not supported by a Merchant Acquirer relationship',
		        'Insufficient funds','Over credit limit','Card reported lost','Card reported stolen','Pick up card','Card not active',
		        'Card not yet effective','Invalid card status','Account closed','Card suspended','Invalid CVV / CVC','Invalid PAN','Invalid PIN',
		        'Invalid card data','Invalid card verification value','Incorrect PIN','Invalid card credentials','Card not recognized'
		    )
		),
		summary AS (
		    -- Step 3: Aggregate the filtered results to count total and successful transactions.
		    -- This uses the more efficient FILTER clause instead of two separate CTEs.
		    SELECT
		        bin,
		        processor_name,
		        COUNT(*) FILTER (WHERE status in ('approved','declined')) AS total,
		        COUNT(*) FILTER (WHERE status = 'approved') AS total_success
		    FROM tarns
		    GROUP BY bin, processor_name
		)
		-- Final Step: Calculate the approval rate and present the final columns.
		SELECT
		    s.bin,
		    s.processor_name,
		    s.total,
		    s.total_success,
		    ROUND(
		        s.total_success::numeric / NULLIF(s.total, 0) * 100, 4
		    ) AS approval_rate
		FROM
		    summary s
		ORDER BY s.bin DESC;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return []
def fetch_bin_processor_ar(start_date, end_date,bin_list,processor_list):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        query = f"""
           WITH first_tx_times AS (
                SELECT
                    txid,
                    MIN(created_datetime) AS first_transaction_datetime
                FROM altitude_transaction
                WHERE created_date BETWEEN '{start_date}' AND '{end_date}'
                AND error_description NOT IN ({errors})
                GROUP BY txid
            ),
            first_transaction AS (
                SELECT DISTINCT ON (at.txid, at.processor_name)
                    at.txid,
                    at.processor_name,
                    at.status,
                    at.created_datetime AS first_transaction_time
                FROM altitude_transaction at
                INNER JOIN first_tx_times ftt
                    ON at.txid = ftt.txid
                    AND at.created_datetime = ftt.first_transaction_datetime
                ORDER BY at.txid, at.processor_name, at.created_datetime
            ),
            transaction_with_card AS (
                SELECT
                    at.txid,
                    at.processor_name,
                    at.status,
                    LEFT(c.card_no, 6) AS BIN
                FROM altitude_transaction at
                INNER JOIN first_transaction ft
                    ON at.txid = ft.txid
                    AND at.processor_name = ft.processor_name
                    AND at.created_datetime = ft.first_transaction_time
                INNER JOIN altitude_customers c
                    ON at.txid = c.txid
                WHERE LEFT(c.card_no, 6) IN ({','.join(f"'{bin}'" for bin in bin_list)})
            ),
            aggregated_result AS (
                SELECT
                    BIN,
                    processor_name AS Processor,
                    COUNT(*) FILTER (WHERE status in ('approved','declined')) AS Total,
                    COUNT(*) FILTER (WHERE status = 'approved') AS Total_Success
                FROM transaction_with_card
                GROUP BY BIN, processor_name
            )
            SELECT
                BIN,
                Processor,
                Total,
		Total_Success,
                ROUND(
		    ((Total_Success::FLOAT / NULLIF(Total, 0)::FLOAT) * 100)::numeric,
		    3
		) AS Ar
            FROM aggregated_result;
        """
        df = pd.read_sql(query, conn)
        conn.close()
	# print(df)
        return df
    except Exception as e:
	    print(e)
	# print(e)
        # return []
