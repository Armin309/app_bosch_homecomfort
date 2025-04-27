import sys
import os
# Add one more parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import psycopg2
from file_interaction.source_config import SFDE_USERNAME, SFDE_PASSWORD, SFDE_DATABASE, SFDE_HOST, SFDE_PORT

def create_materialized_view_unpivoted_sales():
    """
    Creates a materialized view for unpivoted sales data.
    """
    try:
        conn = psycopg2.connect(
            dbname=SFDE_DATABASE,
            user=SFDE_USERNAME,
            password=SFDE_PASSWORD,
            host=SFDE_HOST,
            port=SFDE_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS public.unpivoted_sales_mv AS
        WITH cte AS (
            SELECT * FROM public.sales_summary_table
        )
        SELECT 
            distribution_channel,
            sales_organization,
            billing_doc_date,
            sales_document,
            sales_group,
            material_id,
            material_text,
            customer_no,
            customer_name,
            PALEDGER,
            'Quantity' AS KPI_values,
            Quantity AS payload
        FROM cte
        UNION ALL
        SELECT 
            distribution_channel,
            sales_organization,
            billing_doc_date,
            sales_document,
            sales_group,
            material_id,
            material_text,
            customer_no,
            customer_name,
            PALEDGER,
            'TGS' AS KPI_values,
            TGS AS payload
        FROM cte
        UNION ALL
        SELECT 
            distribution_channel,
            sales_organization,
            billing_doc_date,
            sales_document,
            sales_group,
            material_id,
            material_text,
            customer_no,
            customer_name,
            PALEDGER,
            'TNS' AS KPI_values,
            TNS AS payload
        FROM cte;
        """)

        print("✅ Materialized view 'unpivoted_sales_mv' created successfully.")

    except Exception as e:
        print(f"❌ Error creating materialized view: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    create_materialized_view_unpivoted_sales()