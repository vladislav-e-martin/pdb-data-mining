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

    # Store the chemical names of the relevant entries
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
        # Find only details sections that contain one of these common delimiters (not usually found in chemical formulae or names)
        find_delimiters = re.findall('[,;:\t]*', details)
        confirmed = False
        for delimiter in find_delimiters:
            if delimiter is not None and confirmed is not True:
                # Show the details section to confirm that there are in fact delimiters
                # print("This entry should be delimited by one of the accepted delimiters.")
                # pprint("DETAILS:{0}".format(details))
                delimited_entries.append(entry_id)
                confirmed = True

    entry_index = 0
    for index, row in enumerate(cursor.execute(sort_command)):

        entry_id = row[0]
        details = row[6].lower()

        # All of these sections will be delimited properly. Now we can trust that each delimiter DOES mark the end of a chemical's description
        find_chemical_names = re.findall('(\d+\s*m{1,2}\s+[^,;:\t]*|\d+[.]\d+\s*m{1,2}\s+[^,;:\t]*|'
                                         '\d+\s*[%]\s*[^,;:\t]*|\d+[.]\d+\s*[%]\s*[^,;:\t]*|'
                                         '(peg)\s*\d+[^,;:\t]*|\d+\s*(peg)[^,;:\t]*|(peg)[-]\d+[^,;:\t]*)', details)

        # Check if this structure's entry is delimited properly
        if entry_id == delimited_entries[entry_index]:
            # Find each chemical name used in the structure's crystallization
            for matches in find_chemical_names:
                for name in matches:
                    # Ensure that a chemical name follows the concentration, percentage, or PEG variation
                    if name is not None and len(name) > 1:
                        raw_chemical_name = name

                        find_open_parentheses = re.search('([(])', raw_chemical_name)
                        find_closed_parentheses = re.search('([)])', raw_chemical_name)
                        # Check if only one parentheses is contained in the chemical's name
                        if find_open_parentheses is not None and find_closed_parentheses is None or \
                                                find_open_parentheses is None and find_closed_parentheses is not None:
                            # If this is the case, it is most surely an errant parentheses and it can be removed safely
                            pprint("Removing the errant parentheses!")
                            chemical_name = re.sub('([(].*$|[)].*$)', '', raw_chemical_name)
                            english_delimited_names = re.split('(?:and)', chemical_name)
                        # No changes need to be made to the chemical name as of yet
                        else:
                            no_parentheses_name = raw_chemical_name
                            english_delimited_names = re.split('(?:and)', no_parentheses_name)

                        # Check if the chemical name is delimited by an english word
                        if len(english_delimited_names) > 1:
                            for english_delimited_name in english_delimited_names:
                                period_delimited_names = re.split('(?:\D+\s*[.]\s*\D+)', english_delimited_name)
                                # If it is delimited with a period, split the phrase both by english and period delimiters
                                if len(period_delimited_names) > 1:
                                    for period_delimited_name in period_delimited_names:
                                        chemical_names.append(period_delimited_name)
                                        chemical_ids.append(entry_id)
                                        pprint("The Entry ID of the below chemical: {0}".format(entry_id))
                                        pprint("ENG/PER, Contents related to a single chemical: {0}".format(
                                            period_delimited_name))
                                # If it is not delimited with a period, check if it is delimited with an english word
                                else:
                                    chemical_names.append(english_delimited_name)
                                    chemical_ids.append(entry_id)
                                    pprint("The Entry ID of the below chemical: {0}".format(entry_id))
                                    pprint("ENG, Contents related to a single chemical: {0}".format(
                                        english_delimited_name))
                        # If it is not delimited with a period, try something else
                        else:
                            period_delimited_names = re.split('(?:\D+\s*[.]\s*\D+)', no_parentheses_name)
                            # If it is not delimited with an english word, check if it is delimited with a period
                            if len(period_delimited_names) > 1:
                                for period_delimited_name in period_delimited_names:
                                    chemical_names.append(period_delimited_name)
                                    chemical_ids.append(entry_id)
                                    pprint("The Entry ID of the below chemical: {0}".format(entry_id))
                                    pprint(
                                        "PER, Contents related to a single chemical: {0}".format(period_delimited_name))
                            # It is delimited by neither an english or a period delimiter. Leave it as it is
                            else:
                                chemical_names.append(no_parentheses_name)
                                chemical_ids.append(entry_id)
                                pprint("The Entry ID of the below chemical: {0}".format(entry_id))
                                pprint("REG, Contents related to a single chemical: {0}".format(no_parentheses_name))

                                # no_ph_name = re.sub('((ph|ph=)\s*\d+.*|\s+\d+\s+.*|\s+\d*$)', '', no_numeric_name)
                                # Remove leading and trailing whitespace from this matched string
                                # chemical = trim_paren_match.strip()
            entry_index += 1
    file = open("/home/vlad/Documents/Code/pdb-data-mining/chemical-names-test.txt", 'w')
    for name in chemical_names:
        file.write("{0}. \n".format(index))
        file.write("{0} \n\n".format(name))
    return chemical_names, chemical_ids


def split_results(results, result_ids):
    rows = len(results)
    result_components = [[0 for x in range(5)] for x in range(rows)]
    result_index = 0
    for index, chemical in enumerate(results):
        concentration_search = re.compile('(\d+|\d+[.]\d+)\s*(m{1,2}|[%])\s*')
        try:
            clean_chemical = chemical.strip()
            concentration = concentration_search.match(clean_chemical).group(1)
            unit = concentration_search.match(clean_chemical).group(2)
            unit_and_name = re.sub(concentration, '', clean_chemical, 1)
            only_name = re.sub(unit, '', unit_and_name, 1)
            name = only_name.strip()
            pprint("Concentration: {0}, Unit: {1}, Name: {2}".format(concentration, unit, name))
            if len(name) > 1:
                result_components[result_index][0] = str(uuid.uuid4())
                result_components[result_index][1] = result_ids[index]
                result_components[result_index][2] = concentration
                result_components[result_index][3] = unit
                result_components[result_index][4] = name
                result_index += 1
        except AttributeError:
            pprint("Chemical: {0}".format(chemical))
            pprint("The above is not a useful chemical. No concentration is provided.")
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
