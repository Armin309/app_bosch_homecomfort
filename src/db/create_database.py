import sys
import os
# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from file_interaction.source_config import SFDE_PASSWORD, SFDE_USERNAME, SFDE_DATABASE, SFDE_HOST, SFDE_PORT

def create_database():
    try:
        # Verbinde dich zur Standard-DB 'postgres'
        conn = psycopg2.connect(
            dbname="postgres",
            user=SFDE_USERNAME,
            password=SFDE_PASSWORD,
            host=SFDE_HOST,
            port=SFDE_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Cursor erstellen
        cursor = conn.cursor()

        # Prüfe, ob Datenbank bereits existiert
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (SFDE_DATABASE,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE DATABASE {SFDE_DATABASE};")
            print(f"Datenbank '{SFDE_DATABASE}' wurde erstellt.")
        else:
            print(f"Datenbank '{SFDE_DATABASE}' existiert bereits.")

        # Verbindungen schließen
        cursor.close()
        conn.close()

    except Exception as e:
        print("Fehler beim Erstellen der Datenbank:", e)
