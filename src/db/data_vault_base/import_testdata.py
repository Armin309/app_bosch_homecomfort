import csv
from datetime import datetime
from pathlib import Path
from psycopg2 import connect
import sys
import os
# Add one more parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from file_interaction.source_config import SFDE_USERNAME, SFDE_PASSWORD, SFDE_DATABASE, SFDE_HOST, SFDE_PORT

# Read CSV data
def load_csv_data(file_path):
    data = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader)  # Skip the header row
        for row in reader:
            data.append(row)
    print("📄 Loaded data from the CSV file:")
    for i, row in enumerate(data[:5]):  # Display the first 5 rows for verification
        print(f"Row {i + 1}: {row}")
    return data

# Insert data into the tables
def insert_data(conn, data):
    cursor = conn.cursor()
    try:
        for i, row in enumerate(data):
            # print(f"🔄 Inserting row {i + 1}: {row}")
##############LOAD DATE SHOULD BE THE CURRENT TIMESTAMP ALSO, HERE JUST IMPORTED FROM THE CSV 
            # Insert in Hub Customer
            cursor.execute("""
                INSERT INTO public.hub_customer (customer_no, load_date, record_src)
                VALUES (%s, %s, %s)
                ON CONFLICT (customer_no) DO NOTHING;
            """, (row[0], row[12], row[13]))

            # Insert into Hub Sales Document
            cursor.execute("""
                INSERT INTO public.hub_sales_doc (sales_document, load_date, record_src)
                VALUES (%s, %s, %s)
                ON CONFLICT (sales_document) DO NOTHING;
            """, (row[2], row[12], row[13]))

            # Insert into Hub Material
            cursor.execute("""
                INSERT INTO public.hub_material (material_id, load_date, record_src)
                VALUES (%s, %s, %s)
                ON CONFLICT (material_id) DO NOTHING;
            """, (row[8], row[12], row[13]))

            # Satellites
            cursor.execute("""
                SELECT customer_no_HK FROM public.hub_customer WHERE customer_no = %s
            """, (row[0],))
            customer_no_HK = cursor.fetchone()

            if customer_no_HK:
                cursor.execute("""
                    INSERT INTO public.sat_customer (customer_no_HK, customer_name, load_date, record_src)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (customer_no_HK, load_date) DO NOTHING;
                """, (customer_no_HK[0], row[1], row[12], row[13]))

            # Fetch foreign keys for satellites and links
            cursor.execute("""
                SELECT sales_doc_HK FROM public.hub_sales_doc WHERE sales_document = %s
            """, (row[2],))
            sales_doc_HK = cursor.fetchone()

            cursor.execute("""
                SELECT material_id_HK FROM public.hub_material WHERE material_id = %s
            """, (row[8],))
            material_id_HK = cursor.fetchone()

            if sales_doc_HK and material_id_HK:
                # Convert billing_doc_date to DATE format
                billing_doc_date = datetime.strptime(row[5], '%d.%m.%Y').date()

                # Insert into sat_sale_doc_attributes
                cursor.execute("""
                    INSERT INTO public.sat_sale_doc_attributes (sales_doc_HK, distribution_channel, sales_organization, billing_doc_date, sales_group, PALEDGER, load_date, record_src)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sales_doc_HK, load_date) DO NOTHING;
                """, (sales_doc_HK[0], row[3], row[4], billing_doc_date, row[6], row[7], row[12], row[13]))

                # Insert into sat_material
                cursor.execute("""
                    INSERT INTO public.sat_material (material_id_HK, material_text, SPRAS, load_date, record_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (material_id_HK, load_date) DO NOTHING;
                """, (material_id_HK[0], row[9], row[10], row[12], row[13]))

                # Insert into link_transaction with business keys
                cursor.execute("""
                    INSERT INTO public.link_transaction (
                        sales_doc_HK, customer_id_HK, material_id_HK, 
                        material_id, customer_no, sales_document, 
                        load_date, record_src
                    )
                    VALUES (
                        %s, %s, %s, 
                        %s, %s, %s, 
                        %s, %s
                    )
                    ON CONFLICT (sales_doc_HK, customer_id_HK, material_id_HK) DO NOTHING;
                """, (
                    sales_doc_HK[0], customer_no_HK[0], material_id_HK[0],
                    row[8], row[0], row[2],
                    row[12], row[13]
                ))

                # Insert into sat_transaction_VVR
                cursor.execute("""
                    SELECT transaction_id_HK FROM public.link_transaction 
                    WHERE sales_doc_HK = %s AND customer_id_HK = %s AND material_id_HK = %s
                """, (sales_doc_HK[0], customer_no_HK[0], material_id_HK[0]))
                transaction_id_HK = cursor.fetchone()

                if transaction_id_HK:
                    cursor.execute("""
                        INSERT INTO public.sat_transaction_VVR (transaction_id_HK, VVR03, VVR05, load_date, record_src)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_id_HK, load_date) DO NOTHING;
                    """, (transaction_id_HK[0], row[10], row[11], row[12], row[13]))

        conn.commit()
        print("✅ Data inserted successfully.")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
    finally:
        cursor.close()

# Establish database connection
def create_connection():
    try:
        conn = connect(
            dbname=SFDE_DATABASE,
            user=SFDE_USERNAME,
            password=SFDE_PASSWORD,
            host=SFDE_HOST,
            port=SFDE_PORT
        )
        conn.autocommit = True
        print("🔗 Connected to the database successfully.")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to the database: {e}")
        return None

# Main function to load and insert data
def main(file_path):
    # Establish database connection
    conn = create_connection()
    if not conn:
        print("Database connection failed. Exiting.")
        return

    # Load CSV data
    print(f"Loading data from {file_path}...")
    data = load_csv_data(file_path)

    # Insert data into the database
    print("Inserting data into the database...")
    insert_data(conn, data)

    # Close the connection
    conn.close()
    print("Data import completed successfully.")
