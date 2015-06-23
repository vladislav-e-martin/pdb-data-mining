# LIBRARIES AND PACKAGES

import sqlite3 as SQL
from pprint import pprint

# CONSTANTS

# FUNCTION DECLARATIONS

# Create new database
def connect_database(folder):
    connection = SQL.connect(folder)
    return connection


# Manage existing database 
def add_table(connection, table):
    cursor = connection.cursor()
    # Create a table of the provided name
    create_command = "CREATE TABLE {0} (id text, resolution real, matthews real, percent_solvent real, pH real, " \
                     "temp real, details text)".format(table)
    # Ensure that each row can be identified uniquely from every other row
    unique_command = "CREATE UNIQUE INDEX entry_id ON {0}(id)".format(table)
    try:
        print("Creating table {0}.".format(table))
        cursor.execute(create_command)
    except SQL.OperationalError:
        print("The table {0} already exists!".format(table))
    try:
        print("Creating a unique index.")
        cursor.execute(unique_command)
    except SQL.OperationalError:
        print("This table already has a unique index.")


# Add a value into either a new column if the specified header does not exist or the existing column
# in the current row of the table
def add_column(connection, name, value, header):
    cursor = connection.cursor()
    new_command = "ALTER TABLE {0} ADD COLUMN {1} text".format(name, header)
    existing_command = "INSERT INTO {0}({1}) VALUES (?)".format(name, header)
    try:
        cursor.execute(new_command)
    except SQL.IntegrityError:
        cursor.execute(existing_command, value)


# Add a row of new values to the table
def add_row(connection, name, values):
    cursor = connection.cursor()
    command = "INSERT OR REPLACE INTO {0} VALUES (?,?,?,?,?,?,?)".format(name)
    cursor.executemany(command, values)

# Commit changes to existing database
def commit_changes(connection):
    print("Committing changes...")
    connection.commit()
    print("Changes have been committed. Disconnecting from the database.")
    connection.close()


# Print out the contents of a database, ordered by a specified column
def print_database(connection, table, column):
    cursor = connection.cursor()
    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)
    for row in cursor.execute(sort_command):
        print(row)
