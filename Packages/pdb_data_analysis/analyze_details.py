__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

import sqlite3 as SQL
import re
from Packages.pdb_data_mining import manage_sql as M_SQL
from pprint import pprint

# CONSTANTS

# FUNCTION DECLARATIONS


# no_space_matches = []
# space_matches = []
#
# hyphen_separated_no_space_matches = []
# hyphen_separated_space_matches = []
#
# peg_matches = []

# hyphen_space_search = re.findall('[\d+.\d]m{1,2}[ ][a-z]*[-][a-z]*', lowered_row)
# hyphen_no_space_search = re.findall('[\d+.\d][ ]m{1,2}[ ][a-z]*[-][a-z]*', lowered_row)
#
# no_space_search = re.findall('[\d+.\d]m{1,2}[ ][^,.;)(\t]*', lowered_row)
# space_search = re.findall('[\d+.\d][ ]m{1,2}[ ][^,.;)(\t]*', lowered_row)
#
# peg_search = re.search('(peg)', lowered_row)
# found = False
# # Find all of the entries that contain chemical names containing a hyphen where the concentration
# # is not separated from it's unit by a space
# for match in hyphen_no_space_search:
#     if match is not None:
#         hyphen_separated_no_space_matches.append(match)
#         found = True
# # Find all of the entries that contain chemical names containing a hyphen where the concentration
# # is separated from it's unit by a space
# for match in hyphen_space_search:
#     if match is not None:
#         hyphen_separated_space_matches.append(match)
#         found = True
# # Find all of the entries that contain chemical names where the concentration is not separated
# # from it's unit by a space
# for match in no_space_search:
#     if match is not None:
#         no_space_matches.append(match)
#         found = True
# # Find all of the entries that contain chemical names where the concentration is separated
# # from it's unit by a space
# for match in space_search:
#     if match is not None:
#         space_matches.append(match)
#         found = True
# if peg_search is not None and found == False:
#     peg_matches.append(peg_search)
#     text_file = open("/home/vlad/Documents/Code/pdb-data-mining/containing-peg.txt", "a")
#     text_file.write("The row's contents are displayed below: \n {0}".format(row[6]))
#     text_file.write("\n\n\n\n")
#     text_file.close()

def find_chemical_matches(connection, table, column):
    cursor = connection.cursor()

    space_separated_results = []
    not_space_separated_results = []
    no_match_case_results = []

    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)
    for index, row in enumerate(cursor.execute(sort_command)):
        lowered_row = row[6].lower()

        space_separated = re.findall('[\d+.\d]m{1,2}[ ][^,.;)(\t]*', lowered_row)
        not_space_separated = re.findall('[\d+.\d]m{1,2}[ ][^,.;)(\t]*', lowered_row)

        no_match_case = re.search('(peg)', lowered_row)

        found_match = False
        for match in space_separated:
            if match is not None:
                # Remove the pH values from this matched string
                match = re.sub('(ph{1}[ ][\d+.\d])', '', match)
                # Remove leading and trailing whitespace from this matched string
                match.strip()
                space_separated_results.append(match)
                found_match = True
        for match in not_space_separated:
            if match is not None:
                # Remove the pH values from this matched string
                match = re.sub('(ph{1}[ ][\d+.\d])', '', match)
                # Remove leading and trailing whitespace from this matched string
                match.strip()
                not_space_separated_results.append(match)
                found_match = True

        if no_match_case is not None and not found_match:
            no_match_case_results.append(no_match_case)

    return space_separated_results, not_space_separated_results, no_match_case_results


def store_results(connection, table, results):
    cursor = connection.cursor()


def calculate_frequency_of_chemical_names(connection, table, column):
    cursor = connection.cursor()

    # Create the child table that will store the chemicals and their respective concentrations that were used to grow
    # the crystallized protein structures
    M_SQL.add_table(connection, "chemicals")

    frequency_command = "SELECT {1}, COUNT(*) FROM {0} GROUP BY {1}".format(table, column)

    cursor.execute(frequency_command)
