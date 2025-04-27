import sys
import os
import csv
import psycopg2
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from file_interaction.source_config import SFDE_PASSWORD, SFDE_USERNAME, SFDE_DATABASE, SFDE_HOST, SFDE_PORT
from pathlib import Path




# Verbindung zur PostgreSQL-Datenbank herstellen
def create_connection():
    try:
        conn = psycopg2.connect(
            dbname=SFDE_DATABASE,
            user=SFDE_USERNAME,
            password=SFDE_PASSWORD,
            host=SFDE_HOST,
            port=SFDE_PORT
        )
        conn.autocommit = True
        print("🔗 Verbindung zur Datenbank erfolgreich.")
        return conn
    except Exception as e:
        print(f"❌ Verbindung fehlgeschlagen: {e}")
        return None

# CSV-Daten einlesen
def load_csv_data(file_path):
    data = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader)  # Überspringe die Header-Zeile
        for row in reader:
            data.append(row)
    print("📄 Geladene Daten aus der CSV-Datei:")
    for i, row in enumerate(data[:5]):  # Zeige die ersten 5 Zeilen zur Überprüfung
        print(f"Zeile {i + 1}: {row}")
    return data

# Daten in die Tabellen einfügen
def insert_data(conn, data):
    cursor = conn.cursor()
    try:
        for i, row in enumerate(data):
            print(f"🔄 Einfügen von Zeile {i + 1}: {row}")

            # Einfügen in Hub Customer
            cursor.execute("""
                INSERT INTO public.hub_customer (customer_no, load_date, record_src)
                VALUES (%s, %s, %s)
                ON CONFLICT (customer_no) DO NOTHING;
            """, (row[0], row[12], row[13]))

            # Einfügen in Hub Sales Document
            cursor.execute("""
                INSERT INTO public.hub_sales_doc (sales_document, load_date, record_src)
                VALUES (%s, %s, %s)
                ON CONFLICT (sales_document) DO NOTHING;
            """, (row[2], row[12], row[13]))

            # Einfügen in Hub Material
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
                # Insert into sat_sale_doc_attributes
                cursor.execute("""
                    INSERT INTO public.sat_sale_doc_attributes (sales_doc_HK, distribution_channel, sales_organization, billing_doc_date, sales_group, PALEDGER, load_date, record_src)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sales_doc_HK, load_date) DO NOTHING;
                """, (sales_doc_HK[0], row[3], row[4], row[5], row[6], row[7], row[12], row[13]))

                # Insert into sat_material
                cursor.execute("""
                    INSERT INTO public.sat_material (material_id_HK, material_text, SPRAS, load_date, record_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (material_id_HK, load_date) DO NOTHING;
                """, (material_id_HK[0], row[9], row[10], row[12], row[13]))

                # Insert into link_transaction
                cursor.execute("""
                    INSERT INTO public.link_transaction (sales_doc_HK, customer_id_HK, material_id_HK, load_date, record_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (sales_doc_HK, customer_id_HK, material_id_HK) DO NOTHING;
                """, (sales_doc_HK[0], customer_no_HK[0], material_id_HK[0], row[12], row[13]))

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
        print("✅ Daten erfolgreich eingefügt.")
    except Exception as e:
        print(f"❌ Fehler beim Einfügen der Daten: {e}")
    finally:
        cursor.close()

def main():
    # Hole den Pfad zur CSV-Datei (2 Ebenen nach oben und dann in den data-Ordner)
    file_path = Path(__file__).resolve().parents[2] / 'data' / 'sales_data.csv'
    # CSV-Daten einlesen
    data = load_csv_data(file_path)

    # Verbindung zur Datenbank aufbauen
    conn = create_connection()

    if conn:
        # Daten in die Tabellen einfügen
        insert_data(conn, data)
        print("✅ Alle Daten wurden erfolgreich importiert!")
        conn.close()

if __name__ == '__main__':
    main()
