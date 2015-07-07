__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from Packages.pdb_data_mining import manage_sql as SQL
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
    structure_ids = []

    for index, row in enumerate(cursor.execute(sort_command)):
        entry_id = row[0]
        details = row[6].lower()

        chemical_search = re.findall('(\d+\s*m{1,2}\s+[^,.;:\t\n]*|\d+[.]\d+\s*m{1,2}\s+[^,.;:\t\n]*|'
                                     '\d+\s*[%]\s*[^,.;:\t\n]*|\d+[.]\d+\s*[%]\s*[^,.;:\t\n]*)', details)

        for match in chemical_search:
            if match is not None:
                # Remove the English language delimiters and pH values from this matched string
                pprint(match)
                trim_english_match = re.sub('\s+(and|in|with|or|at|adjusted|buffer|inhibitor).*', '',
                                            match)
                trim_ph_match = re.sub('((ph|ph=)\s*\d+.*|\s+\d+\s+.*|\s+\d*$)', '', trim_english_match)
                trim_paren_match = re.sub('(\s*[(]$)', '', trim_ph_match)
                # Remove leading and trailing whitespace from this matched string
                chemical = trim_paren_match.strip()
                pprint("Chemical: {0}    Entry ID: {1}".format(chemical, entry_id))
                chemical_results.append(chemical)
                structure_ids.append(entry_id)
    return chemical_results, structure_ids


def split_results(results, result_ids):
    rows = len(results)
    result_components = [[0 for x in range(5)] for x in range(rows)]
    result_index = 0
    for index, chemical in enumerate(results):
        concentration_search = re.compile('(\d+|\d+[.]\d+)\s*(m{1,2}|[%])\s*')
        concentration = concentration_search.match(chemical).group(1)
        unit = concentration_search.match(chemical).group(2)
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


def crystallized_with_single_chemical(connection, table, column, chemicals_list):
    cursor = connection.cursor()

    count_command = "SELECT COUNT(*), {1} FROM {0} GROUP BY {1} ORDER BY 1 DESC".format(table, column)
    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)

    print_command = "SELECT * FROM information ORDER BY id".format(column)

    entry_ids = []
    for index, row in enumerate(cursor.execute(count_command)):
        chemical_count = row[0]
        if chemical_count == 1:
            entry_ids.append(row[1])
    pprint("There is a total of {0} entries of that were crystallized with only one chemical.".format(len(entry_ids)))

    found_match = []
    current_index = 0
    match_count = 0
    for index, row in enumerate(cursor.execute(sort_command)):
        entry_id = row[1]
        if entry_id == entry_ids[current_index]:
            chemical_name = row[4]
            escaped_list = map(re.escape, chemicals_list)
            joined_string = '|'.join(escaped_list)
            regex_command = '(?:{0})'.format(joined_string)
            chemical_search = re.search(regex_command, chemical_name)
            if chemical_search is not None:
                # pprint("A match was made!")
                # pprint("{0} as compared to {1}".format(chemicals_list[0], row[4]))
                found_match.append(entry_id)
                match_count += 1
            # pprint("The for loop has ended!")
            current_index += 1
    pprint("There were a total of {0} matches to {1}.".format(match_count, chemicals_list[0]))

    id_index = 0
    file = open("/home/vlad/Documents/Code/pdb-data-mining/ammonium-sulfate.txt", 'w')
    file.write("Ammonium Sulfate Matches \n\n")
    for index, row in enumerate(cursor.execute(print_command)):
        try:
            if row[0] == found_match[id_index]:
                file.write("A match was found in {0}. The details are shown below. \n".format(found_match[id_index]))
                file.write("{0} \n\n".format(row[6]))
                id_index += 1
        except IndexError:
            pprint("We've reached the last element in the found_match list!")
            break

def store_results(connection, table, results):
    SQL.add_table(connection, table, False, True)
    SQL.add_child_row(connection, table, results)


def create_common_chemical_names_list(connection, table, column):
    cursor = connection.cursor()

    frequency_command = "SELECT COUNT(*), {1} FROM {0} GROUP BY {1} ORDER BY 1 DESC".format(table, column)
    file = open("/home/vlad/Documents/Code/pdb-data-mining/"
                "most-common-crystallization-chemicals.txt", 'w')
    for index, row in enumerate(cursor.execute(frequency_command)):
        file.write("{0}. \n".format(index))
        file.write("{1} occurred {0} times \n\n".format(row[0], row[1]))
    file.close()
