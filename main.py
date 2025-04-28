from pathlib import Path
from src.db.data_vault_base.create_database import create_database
from src.db.data_vault_base.create_table import create_all_tables
from src.db.data_vault_base.import_testdata import main as import_testdata
from src.db.business_layer.create_business_layer_views import create_business_layer_views
from src.db.business_layer.create_sales_summary_view import create_sales_summary_view

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

        # Step 4: Create business layer views
        print("Step 4: Creating business layer views...")
        create_business_layer_views()
        print("Business layer views created successfully.")

        # Step 5: Create sales summary view
        print("Step 5: Creating sales summary view...")
        create_sales_summary_view()
        print("Sales summary view created successfully.")

    except Exception as e:
        print(f"An error occurred during the setup process: {e}")

if __name__ == "__main__":
    main()