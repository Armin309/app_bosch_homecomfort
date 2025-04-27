from pathlib import Path
from src.db.data_vault_base.create_database import create_database
from src.db.data_vault_base.create_table import create_all_tables
from src.db.data_vault_base.import_testdata import main as import_testdata
from src.db.business_layer.create_helper_table_sales_summary import create_sales_summary_helper_table
from src.db.business_layer.create_materialized_view_unpivoted_sales import create_materialized_view_unpivoted_sales

# Adjust the file path for data import
DATA_FILE_PATH = Path(__file__).resolve().parent / 'data' / 'sales_data.csv'

def main():
    try:
        # Step 1: Create the database
        print("Step 1: Creating the database...")
        create_database()
        print("Database created successfully.")

        # Step 2: Create all tables
        print("Step 2: Creating all tables...")
        create_all_tables()
        print("All tables created successfully.")

        # Step 3: Import test data
        print("Step 3: Importing test data...")
        import_testdata(DATA_FILE_PATH)
        print("Test data imported successfully.")

        # Step 4: Create helper table
        print("Step 4: Creating helper table...")
        create_sales_summary_helper_table()
        print("Helper table created successfully.")

        # Step 5: Create materialized view
        print("Step 5: Creating materialized view...")
        create_materialized_view_unpivoted_sales()
        print("Materialized view created successfully.")

    except Exception as e:
        print(f"An error occurred during the setup process: {e}")

if __name__ == "__main__":
    main()
