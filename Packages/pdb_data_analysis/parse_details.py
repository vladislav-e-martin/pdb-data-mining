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
                                                                     '\d+\s*[%]|\d+[.]\d+\s*[%]|'
                                                                     '[(]*v/v[)]*|[(]*w/v[)]*|[(]*w/w[)]*\s*)',
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
                                        grammar = re.search('\s+(with|at|in|of|was|were|for|against|to)\s+',
                                                            paren_filter)
                                        words = re.search('buffer|inhibitor|compound|protein|reservoir|saturated|'
                                                          'solution',
                                                          paren_filter)
                                        if grammar is not None or words is not None:
                                            grammar_filter = re.sub('\s*(with.*|at.*|in.*|of.*|was.*|were.*|for.*|'
                                                                    'against.*|to.*|buffer.*|inhibitor.*|compound.*|'
                                                                    'protein.*|reservoir.*|saturated.*|solution.*)\s*',
                                                                    '', paren_filter)
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
                                w_or_v_check = re.search('([(]*v/v[)]*|[(]*w/v[)]*|[(]*w/w[)]*)', grammar_filter)
                                if concentration_check is not None or percent_check is not None or w_or_v_check is not None:
                                    chemical_concentration = grammar_filter.strip()
                                    # pprint("{0} is a concentration! Storing the string.".format(chemical_concentration))
                                    chemical_concentrations.append(chemical_concentration)
                                    concentration_index += 1
                                    continue
                                if concentration_index == (name_index + 1):
                                    chemical_name = grammar_filter.strip()
                                    if "2-methylpentane-2" in chemical_name or "2-methyl-2" in chemical_name:
                                        # pprint("Changing name to 2-methylpentane-2,4-diol...")
                                        chemical_name = "2-methylpentane-2,4-diol"
                                    if chemical_name == "eg":
                                        pprint("Changing name to ethylene glycol")
                                        chemical_name = "ethylene glycol"
                                    # pprint("{0} is a name! Storing the string and it's ID.".format(chemical_name))
                                    chemical_names.append(chemical_name)
                                    chemical_ids.append(entry_id)
                                    name_index += 1
            entry_index += 1

    # Write out the raw chemical names to a separate text file for further analysis
    file = open("/home/vlad/Documents/Code/pdb-data-mining/chemical-names-test.txt", 'w')
    for index, name in enumerate(chemical_names):
        file.write("{0}. \n".format(index))
        file.write("{0} \n".format(chemical_ids[index]))
        file.write("Concentration: ${0}$    Name: ${1}$ \n\n".format(chemical_concentrations[index], name))

    return chemical_concentrations, chemical_names, chemical_ids


def is_name_short(name):
    if len(name) < 2:
        return True
    else:
        return False


def is_name_not_feasible(name):
    if "=" in name:
        return True
    else:
        return False


def is_name_numeric(name):
    only_numeric = re.search('^(\d*)$', name)

    if only_numeric is not None:
        return True
    else:
        return False


def finalize_chemical_golden_reference_list(chemical_concentrations, chemical_names, chemical_ids):
    # First set of refinements (removing false names)
    refined_chemical_names = []
    refined_chemical_concentrations = []
    refined_chemical_ids = []

    pprint("Original length of golden list of chemical names: {0}".format(len(chemical_names)))

    for index, chemical_name in enumerate(chemical_names):
        # Remove any items that CANNOT be chemical names (i.e., spaces, only numbers, anything with an equal sign in it)
        if is_name_short(chemical_name) or is_name_numeric(chemical_name) or is_name_not_feasible(chemical_name):
            pprint("Removing ${0}$ from the golden list of valid chemical names.".format(chemical_name))
            # Remove all information related to that chemical entry
        else:
            refined_chemical_names.append(chemical_name)
            refined_chemical_concentrations.append(chemical_concentrations[index])
            refined_chemical_ids.append(chemical_ids[index])

    pprint("Length of refined golden list of chemical names: {0}".format(len(refined_chemical_names)))

    rows = len(refined_chemical_concentrations)
    chemical_data_components = [[0 for x in range(5)] for x in range(rows)]
    data_index = 0

    for index, full_concentration in enumerate(refined_chemical_concentrations):
        concentration_search = re.findall('(\d+|\d+[.]\d+)\s*(m{1,2}|[%])\s*', full_concentration)
        w_v_search = re.findall('((v/v)|(w/v)|(w/w))\s*', full_concentration)
        # try:
        for concentration in concentration_search:
            value = concentration[0]
            unit = concentration[1]
            chemical_data_components[data_index][0] = str(uuid.uuid4())
            chemical_data_components[data_index][1] = refined_chemical_ids[index]
            chemical_data_components[data_index][2] = value
            chemical_data_components[data_index][3] = unit
            chemical_data_components[data_index][4] = refined_chemical_names[index]
            data_index += 1
        for w_v in w_v_search:
            value = 0
            unit = w_v[0]
            chemical_data_components[data_index][0] = str(uuid.uuid4())
            chemical_data_components[data_index][1] = refined_chemical_ids[index]
            chemical_data_components[data_index][2] = value
            chemical_data_components[data_index][3] = unit
            chemical_data_components[data_index][4] = refined_chemical_names[index]
            data_index += 1
    # pprint("Length of chemical_data_components: {0}".format(len(chemical_data_components)))
    return chemical_data_components


def store_results(connection, table, chemical_data):
    SQL.delete_all_rows(connection, table)
    SQL.add_table(connection, table, False, True)
    SQL.add_child_row(connection, table, chemical_data)


def display_column_frequency(connection, table, column):
    cursor = connection.cursor()

    frequency_command = "SELECT COUNT(*), {1} FROM {0} GROUP BY {1} HAVING COUNT(*) > 9" \
                        " ORDER BY 1 DESC".format(table, column)

    # Write out the frequency of each finalized chemical name to a separate text file for further analysis
    file = open("/home/vlad/Documents/Code/pdb-data-mining/"
                "most-common-crystallization-chemicals.txt", 'w')
    for index, row in enumerate(cursor.execute(frequency_command)):
        file.write("{0}. \n".format(index))
        file.write("${1}$ occurred ${0}$ times \n\n".format(row[0], row[1]))
    file.close()


def chemical_golden_reference_list(connection):
    cursor = connection.cursor()

    create_list_command = "SELECT COUNT(*), chemical_name FROM crystallization_chemicals GROUP BY chemical_name" \
                          " HAVING COUNT(*) > 9 ORDER BY 1 DESC".format()

    golden_list = []

    for row in cursor.execute(create_list_command):
        chemical_name = row[1]
        golden_list.append(chemical_name)

    # Add special names
    special_names = ["k-po4", "amm po4", "nh4 formate", "gycine", "mg(oac)", "tric-cl", "ammonium sufate", "cdcl",
                     "phoshate", "pbs", "k2so4", "mg nitrate", "mgatp", "magnisium sulphate", "rbcl"]

    for name in special_names:
        golden_list.append(name)

    return golden_list


def search_for_chemical(connection, search_list, max_chemical_total):
    cursor = connection.cursor()

    count_command = "SELECT COUNT(*), parent_entry_id FROM crystallization_chemicals GROUP BY parent_entry_id" \
                    " ORDER BY 1 DESC"
    sort_command = "SELECT * FROM crystallization_chemicals ORDER BY parent_entry_id"

    print_command = "SELECT * FROM information ORDER BY id"

    entry_ids = []

    golden_list = chemical_golden_reference_list(connection)
    other_chemicals = []

    matching_entry_ids = []

    good_matches = []

    # Populate a list of chemicals that are NOT the chemical we are searching for
    for golden_name in golden_list:
        # Escape any potential meta-characters so as to not confuse the regex with it's operators
        escaped_list = map(re.escape, search_list)
        # Join all chemical names contained in the search list into one large search
        joined_string = '|'.join(escaped_list)
        # Search for only exact matches to the entire string, not sub-strings within the string
        regex_command = '({0})'.format(joined_string)
        in_search_list = re.search(regex_command, golden_name)
        if in_search_list is not None:
            pass
        else:
            # pprint("Adding {0} to the other_chemicals list".format(golden_name))
            other_chemicals.append(golden_name)

    # First, check if it is already known that there are multiple chemicals associated with this entry
    for index, row in enumerate(cursor.execute(count_command)):
        chemical_count = row[0]
        entry_id = row[1]

        if chemical_count <= max_chemical_total:
            entry_ids.append(entry_id)

    pprint("Original length of entry_ids: {0}".format(len(entry_ids)))

    # Second, check if any of the chemical names that are NOT the queried chemical are mentioned in this entry
    for index, row in enumerate(cursor.execute(print_command)):
        entry_id = row[0]
        try:
            # Does this entry's details section have the correct number of chemicals?
            if entry_id in entry_ids:
                details = row[6].lower()

                # Escape any potential meta-characters so as to not confuse the regex with it's operators
                escaped_list = map(re.escape, other_chemicals)
                # Join all chemical names contained in the search list into one large search
                joined_string = '|'.join(escaped_list)
                # Search for only exact matches to the entire string, not sub-strings within the string
                regex_command = '({0})'.format(joined_string)
                other_chemicals_search = re.search(regex_command, details)

                # This entry contains no other chemical names (i.e., only contains the queried chemical)
                if other_chemicals_search is not None:
                    pass
                    # pprint("Entry ID: {0}".format(entry_id))
                    # pprint("We found ${0}$ in the details section!".format(other_chemicals_search.group(0)))
                else:
                    matching_entry_ids.append(entry_id)
        except IndexError:
            pprint("We've reached the end of the entry_ids list!")
            break

    # Third, ensure that all the chemical names are versions of the queried chemical
    match_count = 0
    for index, row in enumerate(cursor.execute(sort_command)):
        entry_id = row[1]
        try:
            if entry_id in matching_entry_ids:
                # The 5th element in each row of the "crystallization_chemicals" table contains the chemical name
                chemical_name = row[4]

                # Escape any potential meta-characters so as to not confuse the regex with it's operators
                escaped_list = map(re.escape, search_list)
                # Join all chemical names contained in the search list into one large search
                joined_string = '|'.join(escaped_list)
                # Search for only exact matches to the entire string, not sub-strings within the string
                regex_command = '^(?:{0})$'.format(joined_string)
                chemical_search = re.search(regex_command, chemical_name)

                if chemical_search is not None:
                    good_matches.append(entry_id)
                    match_count += 1
        except IndexError:
            pprint("We've reached the end of the matching_entry_ids list!")
            break

    pprint("There were a total of {0} matches to {1}.".format(len(good_matches), search_list[0]))

    file = open("/home/vlad/Documents/Code/pdb-data-mining/{0}.txt".format(search_list[0]), 'w')
    file.write("{0} MATCHES \n\n".format(search_list[0].upper()))
    current_index = 1
    for index, row in enumerate(cursor.execute(print_command)):
        entry_id = row[0]
        details = row[6]
        try:
            if entry_id in good_matches:
                file.write("{0}. \n".format(current_index))
                file.write("A match was found in {0}. The details are shown below. \n".format(entry_id))
                file.write("{0} \n\n".format(details))
                current_index += 1
        except IndexError:
            pprint("We've reached the last element in the found_match list!")
            break

    return good_matches


def export_plot_data(connection, matches, name):
    cursor = connection.cursor()

    command = "SELECT * FROM crystallization_chemicals ORDER BY parent_entry_id"

    file = open("/home/vlad/Documents/Code/pdb-data-mining/{0}-plot-data.txt".format(name), 'w')
    file.write("ENTRY ID\tVALUE\t\tUNIT\n")
    for index, row in enumerate(cursor.execute(command)):
        entry_id = row[1]
        concentration = row[2]
        converted_concentration = 0
        unit = row[3]
        conversion_occurred = False

        if unit == "%":
            converted_concentration = 4 * (concentration / 100)
            conversion_occurred = True
        if unit == "mm":
            converted_concentration = concentration / 1000
            conversion_occurred = True
        # There is only case of this, manually correct it (30.0%(v/v) in the PDBX_DETAILS section)
        if unit == "v/v":
            converted_concentration = 4 * (30.0 / 100)
            conversion_occurred = True

        if conversion_occurred:
            concentration = converted_concentration
            unit = "m"

        try:
            if entry_id in matches:
                file.write("{0}\t\t{1}\t\t{2}\n".format(entry_id, concentration, unit))
        except IndexError:
            pprint("We've reached the last element in the found_match list!")
            break
