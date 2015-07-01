__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from Packages.pdb_data_mining import manage_sql as M_SQL
import sqlite3 as SQL
import re
import uuid
from pprint import pprint

# CONSTANTS

# FUNCTION DECLARATIONS

# Need to keep track of the associated entry_id as well
def find_chemical_matches(connection, table, column):
    cursor = connection.cursor()

    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)

    space_separated_results = []
    space_separated_id = []
    not_space_separated_results = []
    not_space_separated_id = []

    for index, row in enumerate(cursor.execute(sort_command)):
        entry_id = row[0]
        details = row[6].lower()

        space_separated = re.findall('[\d+][ ]m{1,2}[ ][^,.;:\t]*', details)
        not_space_separated = re.findall('[\d+]m{1,2}[ ][^,.;:\t]*', details)

        found_match = False
        for match in space_separated:
            # [\d+][ ](ph)
            if match is not None:
                # Remove the English language delimiters and pH values from this matched string
                trim_english_match = re.sub('[ ](and)[ ] | [ ](at)[ ] | [ ](in)[ ] | (buffer)', '', match)
                trimmed_match = re.sub('(ph)[ ][\d+]', '', trim_english_match)
                alt_trimmed_match = re.sub('(ph\d+)', '', trimmed_match)
                # Remove leading and trailing whitespace from this matched string
                chemical = alt_trimmed_match.strip()
                space_separated_results.append(chemical)
                space_separated_id.append(entry_id)
                found_match = True
        for match in not_space_separated:
            # [\d+][ ](ph)
            if match is not None:
                # Remove the English language delimiters and pH values from this matched string
                trim_english_match = re.sub('[ ](and)[ ] | [ ](at)[ ] | [ ](in)[ ] | (buffer)', '', match)
                trimmed_match = re.sub('(ph)[ ][\d+]', '', trim_english_match)
                alt_trimmed_match = re.sub('(ph\d+)', '', trimmed_match)
                # Remove leading and trailing whitespace from this matched string
                chemical = alt_trimmed_match.strip()
                not_space_separated_results.append(chemical)
                not_space_separated_id.append(entry_id)
                found_match = True

    merged_results = space_separated_results + not_space_separated_results
    merged_ids = space_separated_id + not_space_separated_id
    return merged_results, merged_ids


def find_custom_crystallization_conditions(connection, table, column, include_chemicals, exclude_chemicals):
    cursor = connection.cursor()

    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)

    found_chemicals = []

    for index, row in enumerate(cursor.execute(sort_command)):
        entry_id = row[0]
        details = row[6].lower()

        # ammonium_sulfate_search = re.findall('(ammonium sulfate)', details)
        # ammonium_sulphate_search = re.findall('(ammonium sulphate)', details)
        # nh4_so4_search = re.findall('(nh4so4)', details)
        # peg_search = re.findall('(peg)', details)

        for chemical_name in include_chemicals:
            if chemical_name in row:
                found_chemicals.append(row[6])

    for chemical_name in exclude_chemicals:
        if chemical_name in found_chemicals:
            pass
            # found_chemicals.remove(current_item)


def split_results(results, result_ids):
    rows = len(results)
    result_components = [[0 for x in range(5)] for x in range(rows)]
    for index, chemical in enumerate(results):
        result_components[index][0] = str(uuid.uuid4())
        result_components[index][1] = result_ids[index]
        concentration_search = re.compile('(\d+)\s*(\w+)')
        concentration = concentration_search.match(chemical).group(1)
        unit = concentration_search.match(chemical).group(2)
        result_components[index][2] = concentration
        result_components[index][3] = unit
        original_string = re.sub(concentration, '', chemical, 1)
        no_concentration_string = re.sub(unit, '', original_string, 1)
        name = no_concentration_string.strip()
        result_components[index][4] = name
    return result_components


# Comments here, complete
def store_results(connection, table, results):
    M_SQL.add_table(connection, table, False, True)
    M_SQL.add_child_row(connection, table, results)


# Comments here, complete
def create_common_chemical_names_list(connection, table, column):
    cursor = connection.cursor()

    frequency_command = "SELECT COUNT(*), {1} FROM {0} GROUP BY {1} ORDER BY 1 DESC LIMIT 250".format(table, column)
    file = open("/home/vlad/Documents/Code/pdb-data-mining/"
                "Results of Analysis/most-common-crystallization-chemicals.txt", 'w')
    for index, row in enumerate(cursor.execute(frequency_command)):
        file.write("{0}. \n".format(index))
        file.write("{1} occurred {0} times \n\n".format(row[0], row[1]))
    file.close()
