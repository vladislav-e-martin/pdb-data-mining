# LIBRARIES AND PACKAGES

import sqlite3 as SQL
import os
from pprint import pprint

# CONSTANTS

BASE = "/home/vlad/Documents/Code/pdb-data-mining/Output/"

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
                   "(id int primary key, " \
                   "parent_id text, " \
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


# Create the entry_coordinate_data parent_table
def create_entry_coordinate_data_table(connection):
    cursor = connection.cursor()

    # Create a child table with the provided name
    create_table = "CREATE TABLE entry_coordinate_data " \
                   "(id int, " \
                   "ionic int, " \
                   "parent_id text, " \
                   "sequence_id int," \
                   "residual_id text, " \
                   "atom_id text," \
                   "x real, " \
                   "y real, " \
                   "z real)"

    try:
        pprint("Creating table entry_coordinate_data.")
        cursor.execute(create_table)
    except SQL.OperationalError:
        pprint("The table entry_coordinate_data already exists!")

    unique_index = "CREATE UNIQUE INDEX unique_id ON entry_coordinate_data(id)"
    try:
        pprint("Creating a unique index for entry_coordinate_data.")
        cursor.execute(unique_index)
    except SQL.OperationalError:
        pprint("The table entry_coordinate_data already has a unique index.")


# Add a row of new values to the entry_data table
def add_entry_data_row(connection, values):
    cursor = connection.cursor()
    command = "INSERT OR REPLACE INTO entry_data VALUES (?,?,?)"
    cursor.executemany(command, values)


# Add a row of new values to the crystallization_chemicals table
def add_crystallization_chemicals_row(connection, values):
    cursor = connection.cursor()
    command = "INSERT OR REPLACE INTO crystallization_chemicals VALUES (?,?,?,?,?)"
    cursor.executemany(command, values)


# Add a row of new values to the entry_coordinate_data table
def add_entry_coordinate_data_row(connection, values):
    cursor = connection.cursor()
    command = "INSERT OR REPLACE INTO entry_coordinate_data VALUES (?,?,?,?,?,?,?,?,?)"
    cursor.executemany(command, values)


# Delete all rows of data from the specified table
def delete_all_rows(connection, table):
    cursor = connection.cursor()

    delete_rows = "DELETE FROM {0}".format(table)

    try:
        cursor.execute(delete_rows)
    except SQL.OperationalError:
        pprint("The table {0} has no rows to delete.".format(table))


# Drop the specified table from the specified connected database
def delete_table(connection, table):
    cursor = connection.cursor()

    delete = "DROP TABLE IF EXISTS {0};".format(table)

    pprint("Deleting table {0}.".format(table))
    cursor.executescript(delete)


# Commit changes to existing database
def commit_changes(connection):
    pprint("Committing changes...")
    connection.commit()


# Close the connection to the connected database
def close_database(connection):
    connection.close()