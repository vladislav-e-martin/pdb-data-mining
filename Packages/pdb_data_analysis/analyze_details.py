__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

import sqlite3 as SQL
import re
from pprint import pprint

# CONSTANTS

# FUNCTION DECLARATIONS

# Connect to the database
def connect_database(folder):
	connection = SQL.connect(folder)
	return connection


def find_common_chemicals(connection, table, column):
	cursor = connection.cursor()

	no_space_matches = []
	space_matches = []
	peg_matches = []

	sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)
	for index, row in enumerate(cursor.execute(sort_command)):
		lowered_row = row[6].lower()
		search1 = re.search('[\d+.\d+]m*[ ][a-z]*[ ][a-z]*', lowered_row)
		search2 = re.search('[\d+.\d+][ ]m*[ ][a-z]*[ ][a-z]*', lowered_row)
		search3 = re.search('(peg)', lowered_row)
		found = False
		if search1 is not None:
			no_space_matches.append(search1)
			found = True
		if search2 is not None:
			space_matches.append(search2)
			found = True
		if search3 is not None and found == False:
			peg_matches.append(search3)
			text_file = open("E:/Code/Python Projects/pdb-data-mining/containing_peg.txt", "a")
			text_file.write("{0}. The row's contents are displayed below: \n {1}".format(index, row[6]))
			text_file.write("\n\n\n\n")
			text_file.close()
	return no_space_matches, space_matches, peg_matches
