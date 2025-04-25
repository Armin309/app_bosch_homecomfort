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
    - 3 Hubs: sales_order, customer, material
    - 1 Link: sales_order_customer_material
    - 3 Satelliten: sales_order_details, customer_info, material_info
    """

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
        CREATE TABLE IF NOT EXISTS public.hub_sales_order (
            sales_order_id VARCHAR PRIMARY KEY,
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.hub_customer (
            customer_id VARCHAR PRIMARY KEY,
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.hub_material (
            material_id VARCHAR PRIMARY KEY,
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR NOT NULL
        );
        """,

        # LINK
        """
        CREATE TABLE IF NOT EXISTS public.link_sales_order_customer_material (
            link_id SERIAL PRIMARY KEY,
            sales_order_id VARCHAR NOT NULL,
            customer_id VARCHAR NOT NULL,
            material_id VARCHAR NOT NULL,
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR NOT NULL,
            FOREIGN KEY (sales_order_id) REFERENCES public.hub_sales_order(sales_order_id),
            FOREIGN KEY (customer_id) REFERENCES public.hub_customer(customer_id),
            FOREIGN KEY (material_id) REFERENCES public.hub_material(material_id)
        );
        """,

        # SATELLITEN
        """
        CREATE TABLE IF NOT EXISTS public.sat_sales_order_details (
            sales_order_id VARCHAR NOT NULL,
            order_date DATE,
            amount NUMERIC,
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR NOT NULL,
            PRIMARY KEY (sales_order_id, load_date),
            FOREIGN KEY (sales_order_id) REFERENCES public.hub_sales_order(sales_order_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.sat_customer_info (
            customer_id VARCHAR NOT NULL,
            customer_name VARCHAR,
            region VARCHAR,
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR NOT NULL,
            PRIMARY KEY (customer_id, load_date),
            FOREIGN KEY (customer_id) REFERENCES public.hub_customer(customer_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.sat_material_info (
            material_id VARCHAR NOT NULL,
            material_text VARCHAR,
            category VARCHAR,
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR NOT NULL,
            PRIMARY KEY (material_id, load_date),
            FOREIGN KEY (material_id) REFERENCES public.hub_material(material_id)
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
