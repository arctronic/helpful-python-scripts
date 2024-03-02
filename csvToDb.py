import csv
import sqlite3
import sys
import os
from typing import List

def sanitize_table_name(file_path: str) -> str:
    """
    Generate a valid SQLite table name from a file path.
    Removes problematic characters and replaces spaces and periods with underscores.
    """
    base_name = os.path.basename(file_path)
    table_name = os.path.splitext(base_name)[0]  # Remove file extension
    table_name = table_name.replace('.', '_').replace(' ', '_').replace('-', '_')  # Replace periods, spaces, and hyphens with underscores
    return table_name

def create_table_from_csv_header(cursor, table_name: str, header_row: List[str]) -> None:
    """
    Create a SQLite table based on the header row of a CSV file.
    Encloses the table name in double quotes to handle special characters.
    """
    columns = ', '.join([f'"{column}" TEXT' for column in header_row])  # Ensure column names are quoted
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns})'
    cursor.execute(sql)

def insert_csv_to_table(cursor, table_name: str, csv_file_path: str) -> None:
    """
    Insert rows from a CSV file into a SQLite table.
    """
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header_row = next(reader)  # This reads the header row
        num_columns = len(header_row)
        placeholders = ', '.join(['?'] * num_columns)  # Generate placeholders based on the number of columns
        # Prepare for data insertion, handling data rows with inconsistent column counts
        for row in reader:
            if len(row) == num_columns:
                try:
                    cursor.execute(f"INSERT INTO \"{table_name}\" VALUES ({placeholders})", row)
                except sqlite3.IntegrityError as e:
                    print(f"Error inserting row: {row}. Error: {e}")
            else:
                print(f"Skipping row with incorrect number of fields: {row}")

def add_csvs_to_database(db_name: str, csv_file_paths: List[str]) -> None:
    """
    Add contents of one or more CSV files to a SQLite database.
    """
    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        for csv_file_path in csv_file_paths:
            table_name = sanitize_table_name(csv_file_path)
            with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                header_row = next(reader)
                create_table_from_csv_header(cursor, table_name, header_row)
                csvfile.seek(0)  # Reset file read position
                next(reader)  # Skip header row again
                insert_csv_to_table(cursor, table_name, csv_file_path)
        conn.commit()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py database.db csv1.csv csv2.csv ...")
        sys.exit(1)

    database_name = sys.argv[1]
    csv_paths = sys.argv[2:]
    add_csvs_to_database(database_name, csv_paths)
