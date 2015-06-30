__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from Packages.pdb_data_mining import manage_sql as M_SQL
import sqlite3 as SQL
import re
import uuid
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

    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)

    space_separated_results = []
    space_separated_id = []
    not_space_separated_results = []
    not_space_separated_id = []
    # no_match_case_results = []

    for index, row in enumerate(cursor.execute(sort_command)):
        entry_id = row[0]
        details = row[6].lower()

        space_separated = re.findall('[\d+][ ]m{1,2}[ ][^,.;)(\t]*', details)
        not_space_separated = re.findall('[\d+]m{1,2}[ ][^,.;)(\t]*', details)

        # no_match_case = re.search('(peg)', details)

        found_match = False
        for match in space_separated:
            if match is not None:
                # Remove the pH values from this matched string
                trimmed_match = re.sub('(ph[ ][\d+])', '', match)
                alt_trimmed_match = re.sub('(ph\d+)', '', trimmed_match)
                # Remove leading and trailing whitespace from this matched string
                chemical = alt_trimmed_match.strip()
                space_separated_results.append(chemical)
                space_separated_id.append(entry_id)
                found_match = True
        for match in not_space_separated:
            if match is not None:
                # Remove the pH values from this matched string
                trimmed_match = re.sub('(ph[ ][\d+])', '', match)
                alt_trimmed_match = re.sub('(ph\d+)', '', trimmed_match)
                # Remove leading and trailing whitespace from this matched string
                chemical = alt_trimmed_match.strip()
                not_space_separated_results.append(chemical)
                not_space_separated_id.append(entry_id)
                found_match = True

                # if no_match_case is not None and not found_match:
                # 	no_match_case_results.append(no_match_case)

    merged_results = space_separated_results + not_space_separated_results
    merged_ids = space_separated_id + not_space_separated_id
    return merged_results, merged_ids


def find_high_low(connection, table, column):
    cursor = connection.cursor()

    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)

    total_count = 0

    total_nh4_so4_count = 0
    peg_count = 0

    for index, row in enumerate(cursor.execute(sort_command)):
        total_count += 1
        entry_id = row[0]
        details = row[6].lower()

        ammonium_sulfate_search = re.findall('(ammonium sulfate)', details)
        ammonium_sulphate_search = re.findall('(ammonium sulphate)', details)
        nh4_so4_search = re.findall('(nh4so4)', details)
        peg_search = re.findall('(peg)', details)

        # if any(chemical_name in row for chemical_name in chemicals_list):
        #    pass

        # Have some sort of counting mechanism for each of the matches
        if (len(ammonium_sulfate_search) > 0
            or len(ammonium_sulphate_search) > 0
            or len(nh4_so4_search) > 0
            and len(peg_search) == 0):
            total_nh4_so4_count += 1
        if ((len(ammonium_sulfate_search) == 0
             and len(ammonium_sulphate_search) == 0
             and len(nh4_so4_search) == 0
             and len(peg_search) > 0)):
            peg_count += 1

    pprint("Total count: {0}".format(total_count))
    pprint("NH4SO4 was mentioned exclusively from PEG {0} times.".format(total_nh4_so4_count))
    pprint("PEG was mentioned exclusively from NH4SO4 {0} times.".format(peg_count))


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
def calculate_frequency_of_chemical_names(connection, table, column):
    cursor = connection.cursor()

    frequency_command = "SELECT COUNT(*), {1} FROM {0} GROUP BY {1} ORDER BY 1 DESC LIMIT 200".format(table, column)
    file = open("/home/vlad/Documents/Code/pdb-data-mining/"
                "Results of Analysis/most-common-crystallization-chemicals.txt", 'w')
    for index, row in enumerate(cursor.execute(frequency_command)):
        file.write("{0}. \n".format(index))
        file.write("{1} occurred {0} times \n\n".format(row[0], row[1]))
    file.close()
