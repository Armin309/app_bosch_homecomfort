from src.db.data_vault_base.create_database import create_database
from src.db.data_vault_base.create_table import create_all_tables


if __name__ == "__main__":
    # Datenbank erstellen
    create_database()

    # Tabellen erstellen
    create_all_tables()
