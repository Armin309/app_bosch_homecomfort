# Data Vault Tool

This tool is designed to set up and manage a PostgreSQL database, including creating tables, importing test data, and generating helper tables and materialized views for business analysis.

## Features
- Create and initialize a PostgreSQL database.
- Import test data from CSV files.
- Generate tables and views for advanced data analysis.

## Prerequisites
1. **Python Environment**: Install Python and Conda.
2. **PostgreSQL**: Install PostgreSQL and ensure it is running.
3. **Database Credentials**: Update the `config.ini` file with your PostgreSQL credentials.

## Installation Steps

### 1. Set Up Python Environment
1. Navigate to the project directory:
   ```bash
   cd data_vault_tool
   ```
2. Create and activate the Conda environment:
   ```bash
   conda env create -f env/environment.yaml
   conda activate data_vault_tool_env
   ```

### 2. Install PostgreSQL
1. Download and install PostgreSQL from [https://www.postgresql.org/download/](https://www.postgresql.org/download/).
2. During installation, note down the username, password, host, and port.

### 3. Configure Database Credentials
1. Open the `config.ini` file in the root directory.
2. Update the following fields with your PostgreSQL credentials:
   ```ini
   [database]
   username = YOUR_USERNAME
   password = YOUR_PASSWORD
   host = YOUR_HOST
   port = YOUR_PORT
   database = YOUR_DATABASE_NAME
   ```

## Execution Steps
1. Ensure PostgreSQL is running.
2. Run the main script to set up the database and import data:
   ```bash
   python main.py
   ```
3. Follow the progress updates printed in the terminal.

## Notes
- Ensure the `sales_data.csv` file is present in the `data` folder.
- The tool is designed to handle errors gracefully and provide meaningful feedback during execution.

## License
Unauthorized copying, distribution, or modification of the code in this project, via any medium, is strictly prohibited. All rights reserved by Armin Thies.
