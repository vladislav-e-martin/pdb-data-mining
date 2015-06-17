# LIBRARIES AND PACKAGES

import sqlite3 as SQL

# CONSTANTS

BASE = "/home/vlad/Documents/Database"

# FUNCTION DECLARATIONS

def create_database(folder):
    connection = SQL.connect(folder)
    return connection

def add_table(connection, table):
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE {0}".format(table))
def add_rows(connection, name, values):
    cursor.executemany("INSERT INTO {name} VALUES (?,?,?,?,?,?)", values)

# MAIN BODY

values = [("A", "B", "C", "D", "E"),
          ("F", "G", "H", "I", "J")]

# Create a new database
connection = create_database(BASE)
# Add a new table to the database
add_table(connection, "TABLE NAME")
# Insert a row of values
add_row(connection, "ROW NAME", values)

# Commit the changes to the database
connection.commit()

# Complete - disconnect from the database
connection.close()

