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
def chemical_name_golden_reference_list(connection, table, column):
    cursor = connection.cursor()

    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)

    chemical_results = []
    structure_id = []

    for index, row in enumerate(cursor.execute(sort_command)):
        entry_id = row[0]
        details = row[6].lower()

        chemical_search = re.findall('(\d+\s*m{1,2}\s+[^,.;:\t\n]*|\d+[.]\d+\s*m{1,2}\s+[^,.;:\t\n]*)', details)

        found_match = False
        for match in chemical_search:
            if match is not None:
                # Remove the English language delimiters and pH values from this matched string
                trim_english_match = re.sub('\s+(and|in|with|or|at|buffer|inhibitor).*', '', match)
                trim_ph_match = re.sub('((ph|ph=)\s*\d+.*|\s+\d+\s+.*|\s+\d*$)', '', trim_english_match)
                trim_paren_match = re.sub('(\s*[(]$)', '', trim_ph_match)
                # Remove leading and trailing whitespace from this matched string
                chemical = trim_paren_match.strip()
                if len(chemical) > 5:
                    chemical_results.append(chemical)
                    structure_id.append(entry_id)
                    found_match = True
    return chemical_results, structure_id


def find_custom_crystallization_conditions(connection, table, column, include_chemicals, exclude_chemicals):
    cursor = connection.cursor()

    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)

    found_chemicals = []

    for index, row in enumerate(cursor.execute(sort_command)):
        entry_id = row[0]
        details = row[6].lower()

        for chemical_name in include_chemicals:
            if chemical_name in row:
                found_chemicals.append(row[6])


def split_results(results, result_ids):
    rows = len(results)
    result_components = [[0 for x in range(5)] for x in range(rows)]
    result_index = 0
    for index, chemical in enumerate(results):
        concentration_search = re.compile('(\d+|\d+[.]\d+)\s*(m{1,2})\s*')
        pprint(chemical)
        concentration = concentration_search.match(chemical).group(1)
        pprint(concentration_search.match(chemical).group(0))
        pprint(concentration)
        unit = concentration_search.match(chemical).group(2)
        pprint(unit)
        original_string = re.sub(concentration, '', chemical, 1)
        no_concentration_string = re.sub(unit, '', original_string, 1)
        name = no_concentration_string.strip()
        if len(name) > 1:
            result_components[result_index][0] = str(uuid.uuid4())
            result_components[result_index][1] = result_ids[index]
            result_components[result_index][2] = concentration
            result_components[result_index][3] = unit
            result_components[result_index][4] = name
            result_index += 1
    return result_components


# Comments here, complete
def store_results(connection, table, results):
    M_SQL.add_table(connection, table, False, True)
    M_SQL.add_child_row(connection, table, results)


# Comments here, complete
def create_common_chemical_names_list(connection, table, column):
    cursor = connection.cursor()

    frequency_command = "SELECT COUNT(*), {1} FROM {0} GROUP BY {1} ORDER BY 1 DESC".format(table, column)
    file = open("/home/vlad/Documents/Code/pdb-data-mining/"
                "Results of Analysis/most-common-crystallization-chemicals.txt", 'w')
    for index, row in enumerate(cursor.execute(frequency_command)):
        file.write("{0}. \n".format(index))
        file.write("{1} occurred {0} times \n\n".format(row[0], row[1]))
    file.close()
