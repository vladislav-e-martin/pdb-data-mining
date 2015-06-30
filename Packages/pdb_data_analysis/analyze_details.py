__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

import sqlite3 as SQL
import re
from Packages.pdb_data_mining import manage_sql as M_SQL
from pprint import pprint

# CONSTANTS

# FUNCTION DECLARATIONS

# if peg_search is not None and found == False:
#     peg_matches.append(peg_search)
#     text_file = open("/home/vlad/Documents/Code/pdb-data-mining/containing-peg.txt", "a")
#     text_file.write("The row's contents are displayed below: \n {0}".format(row[6]))
#     text_file.write("\n\n\n\n")
#     text_file.close()

# Need to keep track of the associated entry_id as well
def find_chemical_matches(connection, table, column):
	cursor = connection.cursor()

	space_separated_results = []
	not_space_separated_results = []
	no_match_case_results = []

	sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)
	for index, row in enumerate(cursor.execute(sort_command)):
		lowered_row = row[6].lower()

		space_separated = re.findall('[\d+][ ]m{1,2}[ ][^,.;)(\t]*', lowered_row)
		not_space_separated = re.findall('[\d+]m{1,2}[ ][^,.;)(\t]*', lowered_row)

		no_match_case = re.search('(peg)', lowered_row)

		found_match = False
		for match in space_separated:
			if match is not None:
				# Remove the pH values from this matched string
				new_match = re.sub('(ph[ ][\d+])', '', match)
				re.sub('(ph\d+)', '', new_match)
				# Remove leading and trailing whitespace from this matched string
				new_match.strip()
				space_separated_results.append(new_match)
				found_match = True
		for match in not_space_separated:
			if match is not None:
				# Remove the pH values from this matched string
				new_match = re.sub('(ph[ ][\d+])', '', match)
				re.sub('(ph\d+)', '', new_match)
				# Remove leading and trailing whitespace from this matched string
				new_match.strip()
				not_space_separated_results.append(new_match)
				found_match = True

		if no_match_case is not None and not found_match:
			no_match_case_results.append(no_match_case)

	merged_results = space_separated_results + not_space_separated_results

	return merged_results


def split_results(results):
	rows = len(results)
	result_components = [[0 for x in range(3)] for x in range(rows)]
	for index, chemical in enumerate(results):
		concentration_search = re.compile('(\d+)\s*(\w+)')
		pprint("The whole string: {0}".format(chemical))
		concentration = concentration_search.match(chemical).group(1)
		unit = concentration_search.match(chemical).group(2)
		result_components[index][0] = concentration
		result_components[index][1] = unit
		original_string = re.sub(concentration, '', chemical, 1)
		no_concentration_string = re.sub(unit, '', original_string, 1)
		name = no_concentration_string.strip()
		result_components[index][2] = name
	return result_components


# NOT COMPLETE
def store_results(connection, table, results):
	M_SQL.add_table(connection, table)
	M_SQL.add_child_row(connection, table, results)


# NOT COMPLETE
def calculate_frequency_of_chemical_names(connection, table, column):
	cursor = connection.cursor()

	frequency_command = "SELECT {1}, COUNT(*) FROM {0} GROUP BY {1}".format(table, column)
	cursor.execute(frequency_command)
