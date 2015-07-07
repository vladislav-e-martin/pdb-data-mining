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
def add_table(connection, table, parent, child):
    cursor = connection.cursor()

    # Create a table of the provided name
    create_parent_command = "CREATE TABLE {0} " \
                            "(id text primary key, " \
                            "resolution real, " \
                            "matthews real, " \
                            "percent_solvent real, " \
                            "pH real, " \
                            "temp real, " \
                            "details text)".format(table)
    if parent:
        try:
            pprint("Creating parent table {0}.".format(table))
            cursor.execute(create_parent_command)
        except SQL.OperationalError:
            pprint("The parent table {0} already exists!".format(table))
        # Ensure that each row can be identified uniquely from every other row
        unique_command = "CREATE UNIQUE INDEX entry_id ON {0}(id)".format(table)
        try:
            pprint("Creating a unique index.")
            cursor.execute(unique_command)
        except SQL.OperationalError:
            pprint("This table already has a unique index.")

    create_child_command = "CREATE TABLE {0} " \
                           "(row_id int, " \
                           "parent_entry_id text, " \
                           "concentration real, " \
                           "unit real, " \
                           "chemical_name text)".format(table)
    if child:
        try:
            pprint("Creating child table {0}.".format(table))
            cursor.execute(create_child_command)
        except SQL.OperationalError:
            pprint("The child table {0} already exists!".format(table))


# Add a row of new values to the table
def add_parent_row(connection, name, values):
    cursor = connection.cursor()
    command = "INSERT OR REPLACE INTO {0} VALUES (?,?,?,?,?,?,?)".format(name)
    cursor.executemany(command, values)


# Add a row of new values to the table
def add_child_row(connection, name, values):
    cursor = connection.cursor()
    command = "INSERT OR REPLACE INTO {0} VALUES (?,?,?,?,?)".format(name)
    cursor.executemany(command, values)


def delete_all_rows(connection, name):
	cursor = connection.cursor()
	command = "DELETE FROM {0}".format(name)
	cursor.execute(command)

# Commit changes to existing database
def commit_changes(connection):
    pprint("Committing changes...")
    connection.commit()
    pprint("Changes have been committed. Disconnecting from the database.")
    connection.close()


# Print out the contents of a database, ordered by a specified column
def print_database(connection, table, column):
    cursor = connection.cursor()

    total_count = 0

    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)
    for row in cursor.execute(sort_command):
        pprint(row)
        total_count += 1
    pprint("Total count: {0}".format(total_count))
