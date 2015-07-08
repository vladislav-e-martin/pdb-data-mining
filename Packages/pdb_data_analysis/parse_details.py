__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from Packages.pdb_data_mining import manage_sql as SQL
import re
import uuid
from pprint import pprint

# CONSTANTS

# FUNCTION DECLARATIONS

# Need to keep track of the associated entry_id as well
def create_chemical_golden_reference_list(connection, table, column):
    cursor = connection.cursor()

    sort_command = "SELECT * FROM {0} ORDER BY {1}".format(table, column)

    # Store the chemical concentrations of the relevant entries
    concentration_index = 0
    chemical_concentrations = []
    # Store the chemical names of the relevant entries
    name_index = 0
    chemical_names = []
    # Store the entry ids of the relevant entries
    chemical_ids = []
    # Store the relevant entries with properly delimited details sections
    delimited_entries = []

    # Discover those entries that are properly delimited (DOES NOT ACCOUNT FOR ENTRIES WITH ONLY ONE CHEMICAL PROVIDED, 
    # UNLESS A PH, TEMPERATURE, OR SOME OTHER INFORMATION IS PROVIDED AFTER A DELIMITER)
    for index, row in enumerate(cursor.execute(sort_command)):
        # Store the entry id of the current structure's entry
        entry_id = row[0]
        # Store the details section of the current structure's entry
        details = row[6]
        # Find only details sections that contain one of these common delimiters
        # (not usually found in chemical formulae or names)
        find_delimiters = re.findall('[,;:]*', details)
        confirmed = False
        for delimiter in find_delimiters:
            if delimiter is not None and confirmed is not True:
                delimited_entries.append(entry_id)
                confirmed = True

    entry_index = 0
    for index, row in enumerate(cursor.execute(sort_command)):

        entry_id = row[0]
        details = row[6].lower()

        # All of these sections will be delimited properly. Now we can trust that each delimiter DOES mark the end of a chemical's description
        comma_delimited_remarks = re.split('(?:[,]|[;])', details)

        # Check if this structure's entry is delimited properly
        if entry_id == delimited_entries[entry_index]:
            # Find each chemical name used in the structure's crystallization
            for name in comma_delimited_remarks:
                if name is not None and len(name) > 1:
                    raw_chemical_name = name

                    english_delimited_names = re.split('(?:and)', raw_chemical_name)
                    # Check if the chemical name is delimited by an english word
                    for english_delimited_name in english_delimited_names:
                        period_delimited_names = re.split('(?:[.][^0-9]+)', english_delimited_name)
                        # Check if the chemical name is delimited with a period
                        for period_delimited_name in period_delimited_names:

                            # Check if some sort of concentration is provided
                            molarity = re.search('((\d+|\d+[.]\d+)\s*m{1,2}\s+)', period_delimited_name)
                            percent = re.search('((\d+|\d+[.]\d+)\s*[%])', period_delimited_name)
                            peg = re.search('(peg)', period_delimited_name)

                            # If no form of concentration is provided, the phrase is useless
                            if molarity is None and percent is None and peg is None:
                                # Skip this iteration of the loop so that this name is not stored in the list
                                continue

                            concentration_delimited_names = re.split('(\d+\s*m{1,2}\s+|\d+[.]\d+\s*m{1,2}\s+|'
                                                                     '\d+\s*[%]|\d+[.]\d+\s*[%])',
                                                                     period_delimited_name)
                            # pprint("Concentration Names: {0}".format(concentration_delimited_names))
                            for concentration_delimited_name in concentration_delimited_names:
                                # Now that the strings have been split into their most basic phrases,
                                # it is time to apply substring filters

                                new_name = concentration_delimited_name
                                while True:
                                    ph_filtered = False
                                    while True:
                                        ph = re.search('(ph\s*(\d+|\d+[.]\d+)|ph\s*[=]\s*(\d+|\d+[.]\d+)|'
                                                       'ph\s*[(]\s*(\d+|\d+[.]\d+))', new_name)
                                        if ph is not None:
                                            ph_filter = re.sub('(ph).*', '', new_name)
                                            # pprint("New sans-pH name: {0}".format(ph_filter))
                                            new_name = ph_filter
                                            ph_filtered = True
                                        else:
                                            ph_filter = new_name
                                            break

                                    paren_filtered = False
                                    while True:
                                        open_parentheses = re.search('([(])', ph_filter)
                                        close_parentheses = re.search('([)])', ph_filter)
                                        # Check if only one parentheses is contained in the chemical's name
                                        if open_parentheses is not None and close_parentheses is None or \
                                                                close_parentheses is not None and open_parentheses is None:
                                            # If this is the case, it is surely an errant parentheses and it can be removed safely
                                            paren_filter = re.sub('([(].*|[)].*)', '', ph_filter)
                                            ph_filter = paren_filter
                                            paren_filtered = True
                                        else:
                                            # If this is not the case, make sure the name remains consistent regardless
                                            paren_filter = ph_filter
                                            break

                                    grammar_filtered = False
                                    while True:
                                        grammar = re.search('\s+(with|at|in|of|was|were|for|against|buffer)\s+',
                                                            paren_filter)
                                        if grammar is not None:
                                            grammar_filter = re.sub('\s+(with.*|at.*|in.*|of.*|was.*|were.*|for.*|'
                                                                    'against.*|buffer.*)\s+', '', paren_filter)
                                            paren_filter = grammar_filter
                                            grammar_filtered = True
                                        else:
                                            grammar_filter = paren_filter
                                            break
                                    new_name = grammar_filter

                                    # Filters have been applied completely by this point, break the loop
                                    if not ph_filtered and not paren_filtered and not grammar_filtered:
                                        break

                                # pprint("Current name being analyzed: {0}".format(grammar_filter))
                                # The final chemical name is equivalent to the results of the last filter applied
                                concentration_check = re.search('((\d+|\d+[.]\d+)\s*m{1,2}\s+)', grammar_filter)
                                percent_check = re.search('((\d+|\d+[.]\d+)\s*[%])', grammar_filter)
                                if concentration_check is not None or percent_check is not None:
                                    chemical_concentration = grammar_filter.strip()
                                    # pprint("{0} is a concentration! Storing the string.".format(chemical_concentration))
                                    chemical_concentrations.append(chemical_concentration)
                                    concentration_index += 1
                                    continue
                                if concentration_index == (name_index + 1):
                                    chemical_name = grammar_filter.strip()
                                    # pprint("The Entry ID of the below chemical: {0}".format(entry_id))
                                    # pprint("{0} is a name! Storing the string and it's ID.".format(chemical_name))
                                    chemical_names.append(chemical_name)
                                    chemical_ids.append(entry_id)
                                    name_index += 1
            entry_index += 1
    file = open("/home/vlad/Documents/Code/pdb-data-mining/chemical-names-test.txt", 'w')
    for index, name in enumerate(chemical_names):
        file.write("{0}. \n".format(index))
        file.write("{0} \n".format(chemical_ids[index]))
        file.write("Concentration: ${0}$    Name: ${1}$ \n\n".format(chemical_concentrations[index], name))
    return chemical_concentrations, chemical_names, chemical_ids


def finalize_chemical_golden_reference_list(chemical_concentrations, chemical_names, chemical_ids):
    pprint("Original length of golden list of chemical names: {0}".format(len(chemical_concentrations)))
    for index, chemical_name in enumerate(chemical_names):
        only_numeric = re.search('(^\d*$)', chemical_name)
        if chemical_name == "" or "=" in chemical_name or only_numeric is not None:
            if only_numeric is not None:
                pprint("Only Numeric search returned... {0}".format(only_numeric.group(0)))
            pprint("Removing {0} from the golden list of valid chemical names.".format(chemical_name))
            # Remove all information related to that chemical entry
            del chemical_names[index]
            del chemical_concentrations[index]
            del chemical_ids[index]
    pprint("Refined length of golden list of chemical names: {0}".format(len(chemical_concentrations)))
    rows = len(chemical_concentrations)
    chemical_data_components = [[0 for x in range(5)] for x in range(rows)]
    data_index = 0
    for index, chemical_concentration in enumerate(chemical_concentrations):
        concentration_search = re.findall('(\d+|\d+[.]\d+)\s*(m{1,2}|[%])\s*', chemical_concentration)
        try:
            for concentration in concentration_search:
                value = concentration[0]
                unit = concentration[1]
                # pprint("Concentration: {0}, Unit: {1}".format(value, unit))
                chemical_data_components[data_index][0] = str(uuid.uuid4())
                chemical_data_components[data_index][1] = chemical_ids[index]
                chemical_data_components[data_index][2] = value
                chemical_data_components[data_index][3] = unit
                chemical_data_components[data_index][4] = chemical_names[index]
                data_index += 1
        except AttributeError:
            pprint("Chemical: {0}".format(chemical_names[index]))
            pprint("The above is not a useful chemical. No concentration is provided.")
    return chemical_data_components


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
                found_match.append(entry_id)
                match_count += 1
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


def store_results(connection, table, chemical_data):
    SQL.delete_all_rows(connection, table)
    SQL.add_table(connection, table, False, True)
    SQL.add_child_row(connection, table, chemical_data)


def display_column_frequency(connection, table, column):
    pprint("Entered this function!")
    cursor = connection.cursor()

    frequency_command = "SELECT COUNT(*), {1} FROM {0} GROUP BY {1} HAVING COUNT(*) > 9 ORDER BY 1 DESC".format(table,
                                                                                                                column)
    file = open("/home/vlad/Documents/Code/pdb-data-mining/"
                "most-common-crystallization-chemicals.txt", 'w')
    pprint("Writing things into the text file!")
    for index, row in enumerate(cursor.execute(frequency_command)):
        file.write("{0}. \n".format(index))
        file.write("{1} occurred {0} times \n\n".format(row[0], row[1]))
    file.close()
