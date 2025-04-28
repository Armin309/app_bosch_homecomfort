from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import os
# Add one more parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from file_interaction.source_config import SFDE_HOST, SFDE_PORT, SFDE_USERNAME, SFDE_PASSWORD, SFDE_DATABASE

def create_database():
    try:
        # Connect to the default 'postgres' database
        conn = connect(
            dbname="postgres",
            user=SFDE_USERNAME,
            password=SFDE_PASSWORD,
            host=SFDE_HOST,
            port=SFDE_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Create a cursor
        cursor = conn.cursor()

        # Check if the database already exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (SFDE_DATABASE,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE DATABASE {SFDE_DATABASE};")
            print(f"Database '{SFDE_DATABASE}' created.")
        else:
            print(f"Database '{SFDE_DATABASE}' already exists.")

        # Close the connections
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error creating the database:", e)