__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

import sqlite3 as SQL
import re
from Packages.pdb_data_mining import manage_sql as M_SQL
from pprint import pprint

# CONSTANTS

# FUNCTION DECLARATIONS

# Connect to the database
def connect_database(folder):
    connection = SQL.connect(folder)
    return connection


# SWITCH TO .FINDALL INSTEAD OF .SEARCH
def find_common_chemicals(connection, table, column):
    cursor = connection.cursor()

    no_space_matches = []
    space_matches = []

    hyphen_separated_no_space_matches = []
    hyphen_separated_space_matches = []

    peg_matches = []

    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)
    for index, row in enumerate(cursor.execute(sort_command)):
        lowered_row = row[6].lower()
        hyphen_space_search = re.findall('[\d+.\d+]m*[ ][a-z]*[-][a-z]*', lowered_row)
        hyphen_no_space_search = re.findall('[\d+.\d+][ ]m*[ ][a-z]*[-][a-z]*', lowered_row)

        no_space_search = re.findall('[\d+.\d+]m*[ ]/([^,]+)/*', lowered_row)
        space_search = re.findall('[\d+.\d+][ ]m*[ ]/([^,]+)/', lowered_row)

        peg_search = re.findall('(peg)', lowered_row)
        found = False
        # Find all of the entries that contain chemical names containing a hyphen where the concentration
        # is not separated from it's unit by a space
        if hyphen_no_space_search is not None:
            hyphen_separated_no_space_matches.append(hyphen_no_space_search)
            found = True
        # Find all of the entries that contain chemical names containing a hyphen where the concentration
        # is separated from it's unit by a space
        if hyphen_space_search is not None:
            hyphen_separated_space_matches.append(hyphen_space_search)
            found = True
        # Find all of the entries that contain chemical names where the concentration is not separated
        # from it's unit by a space
        if no_space_search is not None:
            no_space_matches.append(hyphen_space_search)
            found = True
        # Find all of the entries that contain chemical names where the concentration is separated
        # from it's unit by a space
        if space_search is not None:
            space_matches.append(hyphen_space_search)
            found = True
        if peg_search is not None and found == False:
            peg_matches.append(peg_search)
            text_file = open("E:/Code/Python Projects/pdb-data-mining/containing_peg.txt", "a")
            text_file.write("The row's contents are displayed below: \n {0}".format(row[6]))
            text_file.write("\n\n\n\n")
            text_file.close()

    # Create the child table that will store the chemicals and their respective concentrations that were used to grow
    # the crystallized protein structures
    M_SQL.add_table(connection, "chemicals")

    return hyphen_separated_no_space_matches, hyphen_separated_space_matches, no_space_matches, space_matches, peg_matches
