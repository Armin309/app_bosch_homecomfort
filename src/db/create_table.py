import sys
import os
import psycopg2
from psycopg2 import sql

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from file_interaction.source_config import (
    SFDE_USERNAME,
    SFDE_PASSWORD,
    SFDE_DATABASE,
    SFDE_HOST,
    SFDE_PORT,
)

def create_all_tables():
    """
    Erstellt die Tabellen für ein vollständiges Data Vault Modell:
    - 3 Hubs: customer, material, sales_doc
    - 1 Link: transaction
    - 4 Satelliten: sat_material, sat_sale_doc_attributes, sat_transaction_VVR, sat_customer
    """
############################### vergiss nicht die VVR ZU ERGÄNZEN!!!!!!!
    # Verbindung zur Datenbank aufbauen
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
        print("🔗 Verbindung zur Datenbank erfolgreich.")
    except Exception as e:
        print(f"❌ Verbindung fehlgeschlagen: {e}")
        return

    # SQL-Statements für alle Tabellen
    sql_statements = [

        # HUBs
        """
        CREATE TABLE IF NOT EXISTS public.hub_customer (
            customer_no_HK SERIAL PRIMARY KEY,
            customer_no VARCHAR(50) NOT NULL UNIQUE,
            load_date DATE NOT NULL,
            record_src CHAR(50) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.hub_material (
            material_id_HK SERIAL PRIMARY KEY,
            material_id INT NOT NULL UNIQUE,
            load_date DATE NOT NULL,
            record_src CHAR(50) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.hub_sales_doc (
            sales_doc_HK SERIAL PRIMARY KEY,
            sales_document CHAR(50) NOT NULL UNIQUE,
            load_date DATE NOT NULL,
            record_src CHAR(50) NOT NULL
        );
        """,

        # LINK
        """
        CREATE TABLE IF NOT EXISTS public.link_transaction (
            transaction_id_HK SERIAL PRIMARY KEY,
            customer_id_HK INT NOT NULL,
            sales_doc_HK INT NOT NULL,
            material_id_HK INT NOT NULL,
            load_date DATE NOT NULL,
            record_src CHAR(50) NOT NULL,
            UNIQUE (sales_doc_HK, customer_id_HK, material_id_HK),
            FOREIGN KEY (customer_id_HK) REFERENCES public.hub_customer(customer_no_HK),
            FOREIGN KEY (sales_doc_HK) REFERENCES public.hub_sales_doc(sales_doc_HK),
            FOREIGN KEY (material_id_HK) REFERENCES public.hub_material(material_id_HK)
        );
        """,

        # SATELLITEN
        """
        CREATE TABLE IF NOT EXISTS public.sat_material (
            sat_material_id_HK SERIAL PRIMARY KEY,
            material_id_HK INT NOT NULL,
            material_text CHAR(50) NOT NULL,
            SPRAS CHAR(50) NOT NULL,
            load_date DATE NOT NULL,
            record_src CHAR(50) NOT NULL,
            UNIQUE (material_id_HK, load_date),
            FOREIGN KEY (material_id_HK) REFERENCES public.hub_material(material_id_HK)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.sat_sale_doc_attributes (
            sat_sale_doc_attributes_HK SERIAL PRIMARY KEY,
            sales_doc_HK INT NOT NULL,
            distribution_channel CHAR(50) NOT NULL,
            sales_organization CHAR(50) NOT NULL,
            billing_doc_date CHAR(50) NOT NULL,
            sales_group CHAR(50) NOT NULL,
            PALEDGER CHAR(50) NOT NULL,
            load_date DATE NOT NULL,
            record_src CHAR(50) NOT NULL,
            UNIQUE (sales_doc_HK, load_date),
            FOREIGN KEY (sales_doc_HK) REFERENCES public.hub_sales_doc(sales_doc_HK)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.sat_customer (
            sat_customer_id_HK SERIAL PRIMARY KEY,
            customer_no_HK INT NOT NULL,
            customer_name CHAR(50) NOT NULL,
            load_date DATE NOT NULL,
            record_src CHAR(50) NOT NULL,
            UNIQUE (customer_no_HK, load_date),
            FOREIGN KEY (customer_no_HK) REFERENCES public.hub_customer(customer_no_HK)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.sat_transaction_VVR (
            sat_transaction_VVR_id_HK SERIAL PRIMARY KEY,
            transaction_id_HK INT NOT NULL,
            VVR03 FLOAT NOT NULL,
            VVR05 FLOAT NOT NULL,
            load_date DATE NOT NULL,
            record_src CHAR(50) NOT NULL,
            UNIQUE (transaction_id_HK, load_date),
            FOREIGN KEY (transaction_id_HK) REFERENCES public.link_transaction(transaction_id_HK)
        );
        """
    ]

    # Tabellen nacheinander erstellen
    for stmt in sql_statements:
        try:
            cursor.execute(stmt)
            print(f"✅ Tabelle erstellt oder existiert bereits:\n{stmt.strip().splitlines()[0]}")
        except Exception as e:
            print(f"❌ Fehler beim Erstellen der Tabelle:\n{stmt}\nFehler: {e}")

    # Verbindung schließen
    cursor.close()
    conn.close()
    print("🔒 Verbindung geschlossen.")

if __name__ == "__main__":
    create_all_tables()

