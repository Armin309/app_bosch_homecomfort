import sys
import os
# Add one more parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import psycopg2
from file_interaction.source_config import SFDE_USERNAME, SFDE_PASSWORD, SFDE_DATABASE, SFDE_HOST, SFDE_PORT

def create_sales_summary_helper_table():
    """
    Creates a helper table for sales summary and populates it with pre-aggregated data.
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

        # Create the helper table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.sales_summary_table (
            distribution_channel CHAR(50),
            sales_organization CHAR(50),
            billing_doc_date CHAR(50),
            sales_document CHAR(50),
            sales_group CHAR(50),
            material_id INT,
            material_text CHAR(50),
            customer_no CHAR(50),
            customer_name CHAR(50),
            PALEDGER CHAR(50),
            Quantity NUMERIC,
            TGS NUMERIC,
            TNS NUMERIC
        );
        """)

        # Populate the helper table
        cursor.execute("""
        INSERT INTO public.sales_summary_table
        SELECT 
            sat_sale_doc_attributes.distribution_channel,
            sat_sale_doc_attributes.sales_organization,
            sat_sale_doc_attributes.billing_doc_date,
            hub_sales_doc.sales_document,
            sat_sale_doc_attributes.sales_group,
            hub_material.material_id,
            sat_material.material_text,
            hub_customer.customer_no,
            sat_customer.customer_name,
            sat_sale_doc_attributes.PALEDGER,
            SUM(sat_transaction_VVR.VVR03) AS Quantity,
            SUM(sat_transaction_VVR.VVR03 + sat_transaction_VVR.VVR05 * 2) AS TGS,
            SUM(sat_transaction_VVR.VVR03 - sat_transaction_VVR.VVR05 / 2) AS TNS
        FROM 
            public.link_transaction
        LEFT JOIN public.hub_sales_doc 
            ON link_transaction.sales_doc_HK = hub_sales_doc.sales_doc_HK
        LEFT JOIN public.hub_customer 
            ON link_transaction.customer_id_HK = hub_customer.customer_no_HK
        LEFT JOIN public.hub_material 
            ON link_transaction.material_id_HK = hub_material.material_id_HK
        LEFT JOIN public.sat_sale_doc_attributes 
            ON hub_sales_doc.sales_doc_HK = sat_sale_doc_attributes.sales_doc_HK
        LEFT JOIN public.sat_customer 
            ON hub_customer.customer_no_HK = sat_customer.customer_no_HK
        LEFT JOIN public.sat_material 
            ON hub_material.material_id_HK = sat_material.material_id_HK
        LEFT JOIN public.sat_transaction_VVR 
            ON link_transaction.transaction_id_HK = sat_transaction_VVR.transaction_id_HK
        WHERE 
            sat_sale_doc_attributes.PALEDGER = '01'
            AND sat_sale_doc_attributes.sales_organization IN ('SalesOrg1', 'SalesOrg2', 'SalesOrg3')
            AND sat_sale_doc_attributes.distribution_channel IN ('Retail', 'Wholesale')
            AND sat_sale_doc_attributes.billing_doc_date = '14.08.2023'
        GROUP BY 
            sat_sale_doc_attributes.distribution_channel,
            sat_sale_doc_attributes.sales_organization,
            sat_sale_doc_attributes.billing_doc_date,
            hub_sales_doc.sales_document,
            sat_sale_doc_attributes.sales_group,
            hub_material.material_id,
            sat_material.material_text,
            hub_customer.customer_no,
            sat_customer.customer_name,
            sat_sale_doc_attributes.PALEDGER;
        """)

        print("✅ Helper table 'sales_summary_table' created and populated successfully.")

    except Exception as e:
        print(f"❌ Error creating helper table: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    create_sales_summary_helper_table()