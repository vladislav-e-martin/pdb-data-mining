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
        pprint("Matches: {0} and {1}".format(concentration, unit))
        result_components[index][0] = concentration
        result_components[index][1] = unit
        name = re.sub(concentration, '', chemical, 1)
        new_name = re.sub(unit, '', name, 1)
        s_name = new_name.strip()
        result_components[index][2] = s_name

        pprint("Reduced string: {0}".format(s_name))
    return result_components
    # result_components[index][0] = value_match.group(1)
    # result_components[index][1] = value_match.group(2)
    # pprint("Concentration: {0}".format(result_components[index][0]))
    # pprint("Concentration's unit: {0}".format(result_components[index][1]))

    # concentration_matches = re.compile('([\d+.\d])')
    # concentration_match = concentration_matches.search(chemical)
    # chemical.replace(concentration_match.group(1), '')
    # result_components[index][0] = concentration_match.group(1)
    # pprint("Concentration: {0}".format(result_components[index][0]))
    # pprint("New chemical string: {0}".format(chemical))
    #
    # unit_matches = re.compile('(m+)')
    # unit_match = unit_matches.search(chemical)
    # chemical.replace(unit_match.group(1), '')
    # result_components[index][1] = unit_match.group(1)
    # pprint("Concentration's unit: {0}".format(result_components[index][1]))
    # pprint("New chemical string: {0}".format(chemical))
    # name = chemical
    # result_components[index][2] = name

    # for index in range(len(result_components)):
    #     pprint("The current index: {0}".format(index))
    #     pprint("The concentration: {0}, The concentration's unit: {1}, The chemical's name: {2}"
    #            .format(result_components[index][0],
    #                    result_components[index][1],
    #                    result_components[index][2]))


def store_results(connection, table, results):
    M_SQL.add_table(connection, table)
    M_SQL.add_child_row(connection, table, results)

def calculate_frequency_of_chemical_names(connection, table, column):
    cursor = connection.cursor()

    frequency_command = "SELECT {1}, COUNT(*) FROM {0} GROUP BY {1}".format(table, column)

    pprint(cursor.execute(frequency_command))
