# ETL Pipeline Project

## Overview
This project is an ETL (Extract, Transform, Load) pipeline designed to process and transform data from MySQL database to PostgreSQL database in a structured format for analysis and reporting.

## Features
- Data extraction from MySQL database
- Data transformation and cleaning. Steps involved:
    - Merging tables
    - Converting integer to string
    - Creating new columns
    - Changing cases to title and lower cases
    - Removing appropriate rows with null values
    - Selecting required columns
    - Renaming columns
- Data loading into a PostgreSQL database
- Configurable and scalable

## Prerequisites
- Python 3.13.1
- Required Python libraries (listed in `requirements.txt`)
- Source database is from MySQL sample database called sakila
- Target database is created using `utils/loading.py` file

## Installation
1. Clone the repository:
    ```bash
    git clone https://github.com/ayondeyy/etl_pipeline.git
    ```
2. Navigate to the project directory:
    ```bash
    cd etl_pipeline
    ```
3. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration
Create and configure the source and target database in the  `database_connection.json` file in the root of the project. The contents should look like this:
```bash
{
    "mysql_username": "your_username",
    "mysql_password": "your_password",
    "mysql_host": "localhost",
    "mysql_port": "3306",
    "mysql_database_name": "sakila",
    "pg_username": "your_username",
    "pg_password": "your_password",
    "pg_host": "localhost",
    "pg_port": "5432",
    "pg_database_name": "movie"
}
```
Note: Do not change the value of `mysql_database_name`. The `pg_database_name` variable can be anything.

## Usage
Run the ETL pipeline:
```bash
python main.py
```

## License
This project is licensed under the MIT License.

## Contact
For any questions or issues, please contact the project maintainer directly at [LinkedIn](https://www.linkedin.com/in/ayondeyy/).