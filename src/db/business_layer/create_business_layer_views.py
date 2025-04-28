import psycopg2
import sys
import os
# Add one more parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from file_interaction.source_config import SFDE_USERNAME, SFDE_PASSWORD, SFDE_DATABASE, SFDE_HOST, SFDE_PORT
from datetime import datetime, timedelta
# Add one more parent directory to path

def create_business_layer_views():
    """
    Creates views for the business layer by joining each hub with its corresponding satellite.
    Additionally, creates a view for transactions with aggregated metrics.
    The views exclude the load_date and record_src columns.
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

        # Define the time range (5 years back from today)############## The part below regarding end_date, start_date 
        # is legacy from testing
        # and is not used as it is not the standard business use case to filter history date for a range
        end_date = datetime.today()
        start_date = end_date - timedelta(days=5*365)

        # SQL statements to create views
        views = {
            "customer_DIM": """
                CREATE OR REPLACE VIEW public.customer_DIM AS
                SELECT h.customer_no, s.customer_name
                FROM public.hub_customer h
                LEFT JOIN (
                    SELECT customer_no_HK, customer_name
                    FROM public.sat_customer
                    WHERE (customer_no_HK, load_date) IN (
                        SELECT customer_no_HK, MAX(load_date)
                        FROM public.sat_customer
                        GROUP BY customer_no_HK
                    )
                ) s
                ON h.customer_no_HK = s.customer_no_HK;
            """,
            "material_DIM": """
                CREATE OR REPLACE VIEW public.material_DIM AS
                SELECT h.material_id, s.material_text, s.SPRAS
                FROM public.hub_material h
                LEFT JOIN (
                    SELECT material_id_HK, material_text, SPRAS
                    FROM public.sat_material
                    WHERE (material_id_HK, load_date) IN (
                        SELECT material_id_HK, MAX(load_date)
                        FROM public.sat_material
                        GROUP BY material_id_HK
                    )
                ) s
                ON h.material_id_HK = s.material_id_HK;
            """,
            "sales_doc_DIM": """
                CREATE OR REPLACE VIEW public.sales_doc_DIM AS
                SELECT h.sales_document, s.distribution_channel, s.sales_organization, s.billing_doc_date, s.sales_group, s.PALEDGER
                FROM public.hub_sales_doc h
                LEFT JOIN (
                    SELECT sales_doc_HK, distribution_channel, sales_organization, billing_doc_date, sales_group, PALEDGER
                    FROM public.sat_sale_doc_attributes
                    WHERE (sales_doc_HK, load_date) IN (
                        SELECT sales_doc_HK, MAX(load_date)
                        FROM public.sat_sale_doc_attributes
                        GROUP BY sales_doc_HK
                    )
                ) s
                ON h.sales_doc_HK = s.sales_doc_HK;
            """,
            "transaction_FAC": """
                CREATE OR REPLACE VIEW public.transaction_FAC AS
                SELECT 
                    l.material_id AS material_id,
                    l.customer_no AS customer_no,
                    l.sales_document AS sales_document,
                    SUM(s.VVR03) AS Quantity,
                    SUM(s.VVR03 + s.VVR05 * 2) AS TGS,
                    SUM(s.VVR03 - s.VVR05 / 2) AS TNS
                FROM public.link_transaction l
                LEFT JOIN (
                    SELECT transaction_id_HK, VVR03, VVR05
                    FROM public.sat_transaction_VVR
                    WHERE (transaction_id_HK, load_date) IN (
                        SELECT transaction_id_HK, MAX(load_date)
                        FROM public.sat_transaction_VVR
                        GROUP BY transaction_id_HK
                    )
                ) s
                ON l.transaction_id_HK = s.transaction_id_HK
                GROUP BY l.material_id, l.customer_no, l.sales_document;
            """
        }

        # Execute each view creation statement
        for view_name, view_sql in views.items():
            cursor.execute(view_sql, (start_date, end_date))
            print(f"✅ View '{view_name}' created successfully.")

    except Exception as e:
        print(f"❌ Error creating views: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()