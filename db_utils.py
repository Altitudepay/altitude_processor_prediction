
# db_utils.py
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def run_bin_query():
    query = """
        SELECT DISTINCT LEFT(card_no, 6) AS bin
        FROM public.altitude_transaction t
        LEFT JOIN public.altitude_project p ON t.project_id = p.project_id
        LEFT JOIN public.altitude_customers c ON t.txid = c.txid
        WHERE EXTRACT(YEAR FROM t.created_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')
          AND EXTRACT(MONTH FROM t.created_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
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
    query = """
        SELECT DISTINCT t.processor_name
        FROM public.altitude_transaction t
        WHERE EXTRACT(YEAR FROM t.created_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')
          AND EXTRACT(MONTH FROM t.created_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
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
        query = """
            WITH tarns AS (
                SELECT
                    LEFT(c.card_no, 6) AS bin,
                    t.processor_name,
                    t.status
                FROM public.altitude_transaction t
                INNER JOIN public.altitude_customers c ON t.txid = c.txid
                WHERE t.created_date >= CURRENT_DATE - INTERVAL '1 month'
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
            SELECT
                LEFT(c.card_no, 6) AS BIN,
                t.processor_name as Processor,
                COUNT(t.processor_name) AS Total,
                (SUM(CASE WHEN t.status = 'approved' THEN 1 ELSE 0 END)::FLOAT / COUNT(t.processor_name) * 100) AS Ar
                FROM public.altitude_transaction t
                LEFT JOIN public.altitude_customers c ON t.txid = c.txid
                WHERE 
                t.created_date BETWEEN '{start_date}' AND '{end_date}' AND 
                LEFT(c.card_no, 6) IN ({','.join(f"'{bin}'" for bin in bin_list)}) AND
                t.processor_name IN ({','.join(f"'{processor}'" for processor in processor_list)})
                GROUP BY 
                LEFT(c.card_no, 6),
                t.processor_name;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return []