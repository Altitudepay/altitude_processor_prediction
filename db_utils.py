
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
    SELECT
        txid,
        MIN(created_datetime) AS first_transaction_datetime
    FROM altitude_transaction
    WHERE created_date >= CURRENT_DATE - INTERVAL '1 month' 
    GROUP BY txid
),tarns AS (
		SELECT
			LEFT(c.card_no, 6) AS bin,
			t.processor_name,
			t.status
		FROM public.altitude_transaction t
		INNER JOIN public.altitude_customers c ON t.txid = c.txid
		INNER JOIN first_tx_times ftt
        ON t.txid = ftt.txid
       	AND t.created_datetime = ftt.first_transaction_datetime
		--WHERE t.created_date >= CURRENT_DATE - INTERVAL '1 month' 
	),
	Total_trans AS (
		SELECT
			bin,
			processor_name,
			COUNT(*) AS total
		FROM tarns
		GROUP BY bin, processor_name
	),
	Total_success_trans AS (
		SELECT
			bin,
			processor_name,
			COUNT(*) AS total_success
		FROM tarns
		WHERE status = 'approved'
		GROUP BY bin, processor_name
	)
	SELECT
		t.bin,
		t.processor_name,
		t.total,
		COALESCE(s.total_success, 0) AS total_success,
		ROUND(
			COALESCE(s.total_success::numeric, 0) / NULLIF(t.total, 0) * 100, 4
		) AS approval_rate
	FROM
		Total_trans t
	LEFT JOIN
		Total_success_trans s
		ON t.bin = s.bin AND t.processor_name = s.processor_name order by t.bin desc
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
		    FROM altitude_transaction t
		    WHERE t.created_date BETWEEN '{start_date}' AND '{end_date}' AND AND t.error_description NOT IN ({errors})
			
		    GROUP BY txid
		),
		first_transaction_with_details AS (
		    SELECT
			at.txid,
			at.processor_name,
			c.card_no,
			at.status
		    FROM altitude_transaction at
		    INNER JOIN first_tx_times ftt
			ON at.txid = ftt.txid
		       AND at.created_datetime = ftt.first_transaction_datetime
		    INNER JOIN altitude_customers c
			ON at.txid = c.txid
			WHERE LEFT(c.card_no, 6) IN ({','.join(f"'{bin}'" for bin in bin_list)}) 
		)
		SELECT
		    LEFT(f.card_no, 6) AS BIN,
		    f.processor_name AS Processor,
		    COUNT(*) AS Total,
		    --SUM(CASE WHEN f.status = 'approved' THEN 1 ELSE 0 END) AS success,
		    ROUND(
			(SUM(CASE WHEN f.status = 'approved' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)*100)::NUMERIC,
			3
		    ) AS Ar
		FROM first_transaction_with_details f
		GROUP BY bin, f.processor_name;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return []
