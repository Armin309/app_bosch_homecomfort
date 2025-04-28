import psycopg2
import sys
import os
# Add one more parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from file_interaction.source_config import SFDE_USERNAME, SFDE_PASSWORD, SFDE_DATABASE, SFDE_HOST, SFDE_PORT

def create_sales_summary_view():
    """
    Creates a view for sales summary using business layer views and transaction_FAC.
    """
    try:
        # Establish database connection
        conn = psycopg2.connect(
            dbname=SFDE_DATABASE,
            user=SFDE_USERNAME,
            password=SFDE_PASSWORD,
            host=SFDE_HOST,
            port=SFDE_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create the sales summary view using business layer views
        cursor.execute("""
        CREATE OR REPLACE VIEW public.sales_summary_v_unpivot AS
        SELECT 
            sales_doc_DIM.distribution_channel,
            sales_doc_DIM.sales_organization,
            sales_doc_DIM.billing_doc_date,
            sales_doc_DIM.sales_document,
            sales_doc_DIM.sales_group,
            transaction_FAC.material_id,
            material_DIM.material_text,
            transaction_FAC.customer_no,
            customer_DIM.customer_name,
            unpivoted.kpi_value AS kpi_value,
            unpivoted.payload AS payload
        FROM 
            public.transaction_FAC
        LEFT JOIN public.material_DIM 
            ON transaction_FAC.material_id = material_DIM.material_id
        LEFT JOIN public.customer_DIM 
            ON transaction_FAC.customer_no = customer_DIM.customer_no
        LEFT JOIN public.sales_doc_DIM 
            ON transaction_FAC.sales_document = sales_doc_DIM.sales_document
        CROSS JOIN LATERAL (
            VALUES
                ('Quantity', transaction_FAC.Quantity),
                ('TGS', transaction_FAC.TGS),
                ('TNS', transaction_FAC.TNS)
        ) AS unpivoted(kpi_value, payload)
        WHERE 
            payload IS NOT NULL
            AND sales_doc_DIM.PALEDGER = '01'
            AND sales_doc_DIM.sales_organization IN ('SalesOrg1', 'SalesOrg2', 'SalesOrg3')
            AND sales_doc_DIM.distribution_channel IN ('Retail', 'Wholesale')
            AND sales_doc_DIM.billing_doc_date BETWEEN '2023-08-14' AND '2025-04-28'
        ORDER BY 
            CASE 
                WHEN unpivoted.kpi_value = 'TGS' THEN 1
                WHEN unpivoted.kpi_value = 'TNS' THEN 2
                WHEN unpivoted.kpi_value = 'Quantity' THEN 3
                ELSE 4
            END;
        """)

        print("✅ View 'sales_summary_v_unpivot' created successfully using business layer views.")

    except Exception as e:
        print(f"❌ Error creating view: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()