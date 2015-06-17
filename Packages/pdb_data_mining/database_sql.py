# LIBRARIES AND PACKAGES
 
import sqlite3 as SQL
 
# CONSTANTS
 
BASE = "E:\Code\Python Projects\pdb-data-mining\Database\proteins.db"
 
# FUNCTION DECLARATIONS
 
# Create new database
def connect_database(folder):
    connection = SQL.connect(folder)
    return connection
 
# Manage existing database 
def add_table(connection, table):
    cursor = connection.cursor()
    # Create a table of the provided name
    command = "CREATE TABLE {0} (id text, atoms real, resolution real, method text, information text)".format(table)
    # Ensure that each row can be identified uniquely from every other row
    unique_command = "CREATE UNIQUE INDEX u_index ON {0}(id)".format(table)
    try:
        print("Creating table {0}.".format(table))
        cursor.execute(command)
    except:
        print("The table {0} already exists!".format(table))
    try:
        print("Creating a unique index.")
        cursor.execute(unique_command)
    except:
        print("This table already has a unique index.")
def add_row(connection, name, values):
    cursor = connection.cursor()
    command = "INSERT OR REPLACE INTO {0} VALUES (?,?,?,?,?)".format(name)
    print(command)
    cursor.executemany(command, values)
 
# Commit changes to existing database
def commit_changes(connection):
    print("Committing changes...")
    connection.commit()
    print("Changes have been committed. Disconnecting from the database.")
    connection.close()
 
# Testing purposes
def print_database(connection):
    cursor = connection.cursor()
    for row in cursor.execute("SELECT * FROM proteins ORDER BY resolution"):
        print(row)
 
# MAIN BODY
 
listing = [("0A41", "0", "1.80", "X-RAY DIFFRACTION", "blah"),
            ("4ATX", "306", "2.40", "X-RAY DIFFRACTION", "blahblah")]
 
# Create a new database
connection = connect_database(BASE)
# Add a new table to the database
add_table(connection, "information")
# Insert a row of values
add_row(connection, "information", listing)
 
print_database(connection)
 
# Commit changes and disconnect from the datbase
commit_changes(connection)

