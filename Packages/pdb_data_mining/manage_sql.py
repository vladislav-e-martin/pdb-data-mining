# LIBRARIES AND PACKAGES

import sqlite3 as SQL
from pprint import pprint

# CONSTANTS

# FUNCTION DECLARATIONS

# Create new database
def connect_database(folder):
    connection = SQL.connect(folder)
    return connection


# Create the entry_data parent table
def create_entry_data_table(connection):
    cursor = connection.cursor()

    # Create a parent table with the provided name
    create_table = "CREATE TABLE entry_data " \
                   "(id text primary key, " \
                   "resolution real, " \
                   "matthews real, " \
                   "percent_solvent real, " \
                   "pH real, " \
                   "temp real, " \
                   "details text)"

    try:
        pprint("Creating table entry_data.")
        cursor.execute(create_table)
    except SQL.OperationalError:
        pprint("The table entry_data already exists!")

    # Ensure that each row can be identified uniquely from every other row
    unique_index = "CREATE UNIQUE INDEX unique_id ON entry_data(id)"
    try:
        pprint("Creating a unique index for entry_data.")
        cursor.execute(unique_index)
    except SQL.OperationalError:
        pprint("The table entry_data already has a unique index.")


# Create the crystallization_chemicals child table (child to entry_data)
def create_crystallization_chemicals_table(connection):
    cursor = connection.cursor()

    # Create a child table with the provided name
    create_table = "CREATE TABLE crystallization_chemicals " \
                   "(id int, " \
                   "parent_entry_id text, " \
                   "concentration real, " \
                   "unit real, " \
                   "name text)"

    try:
        pprint("Creating table crystallization_chemicals.")
        cursor.execute(create_table)
    except SQL.OperationalError:
        pprint("The table crystallization_chemicals already exists!")

    unique_index = "CREATE UNIQUE INDEX unique_id ON crystallization_chemicals(id)"
    try:
        pprint("Creating a unique index for crystallization_chemicals.")
        cursor.execute(unique_index)
    except SQL.OperationalError:
        pprint("The table crystallization_chemicals already has a unique index.")


# Create the golden_chemical_reference child table (child to crystallization_chemicals)
def create_golden_chemical_reference_table(connection):
    cursor = connection.cursor()

    # Create a child table with the provided name
    create_table = "CREATE TABLE golden_chemical_reference " \
                   "(id int, " \
                   "parent_id int, " \
                   "name text, " \
                   "frequency int)"

    try:
        pprint("Creating table golden_chemical_reference.")
        cursor.execute(create_table)
    except SQL.OperationalError:
        pprint("The table golden_chemical_reference already exists!")

    unique_index = "CREATE UNIQUE INDEX unique_id ON golden_chemical_reference(id)"
    try:
        pprint("Creating a unique index for golden_chemical_reference.")
        cursor.execute(unique_index)
    except SQL.OperationalError:
        pprint("The table golden_chemical_reference already has a unique index.")


# Add a row of new values to the table
def add_entry_data_row(connection, values):
    cursor = connection.cursor()
    command = "INSERT OR REPLACE INTO entry_data VALUES (?,?,?,?,?,?,?)"
    cursor.executemany(command, values)


# Add a row of new values to the table
def add_crystallization_chemicals_row(connection, values):
    cursor = connection.cursor()
    command = "INSERT OR REPLACE INTO crystallization_chemicals VALUES (?,?,?,?,?)"
    cursor.executemany(command, values)


# Add a row of new values to the table
def add_golden_chemical_reference_row(connection, values):
    cursor = connection.cursor()
    command = "INSERT OR REPLACE INTO golden_chemical_reference VALUES (?,?,?,?)"
    cursor.executemany(command, values)


def delete_all_rows(connection, table):
    cursor = connection.cursor()

    delete_rows = "DELETE FROM {0}".format(table)

    try:
        cursor.execute(delete_rows)
    except SQL.OperationalError:
        pprint("The table {0} has no rows to delete.".format(table))


def delete_table(connection, table):
    cursor = connection.cursor()

    delete = "DELETE {0}".format(table)

    try:
        cursor.execute(delete)
    except SQL.OperationalError:
        pprint("The table {0} does not exist and cannot be deleted.".format(table))

# Commit changes to existing database
def commit_changes(connection):
    pprint("Committing changes...")
    connection.commit()
    pprint("Changes have been committed. Disconnecting from the database.")


def close_database(connection):
    connection.close()


# Print out the contents of a database, ordered by a specified column
def print_database(connection, table, column):
    cursor = connection.cursor()

    total_count = 0

    print_db = "SELECT * FROM {0} ORDER BY unique_id".format(table)

    for row in cursor.execute(print_db):
        pprint(row)
        total_count += 1

    pprint("Total count: {0}".format(total_count))
