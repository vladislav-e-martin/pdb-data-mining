__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

import re
import os
from pprint import pprint

# CONSTANTS

BASE = "/home/vlad/Documents/Code/pdb-data-mining/Output/"

# FUNCTION DECLARATIONS

aliases = {"2-methylpentane-2": "2-methylpentane-2,4-diol",
           "2-methyl-2": "2-methylpentane-2,4-diol",
           "eg": "ethylene glycol",
           "e g": "ethylene glycol",
           "etgly": "ethylene glycol",
           "et gly": "ethylene glycol",
           "ethyleneglycol": "ethylene glycol"}


def standardize_names(connection):
    cursor = connection.cursor()

    select = "SELECT name FROM crystallization_chemicals"

    for row in cursor.execute(select).fetchall():
        name = row[0]
        for alias in aliases.keys():
            regex = '^({0})$'.format(alias)
            alias_search = re.search(regex, name)
            if alias_search is not None:
                standard_name = aliases[name]

                pprint("Standard name: {0}".format(standard_name))
                pprint("Current alias: {0}".format(name))

                update = "UPDATE crystallization_chemicals SET name = ? WHERE name = ?"
                cursor.execute(update, (standard_name, name))


def create_list(connection):
    cursor = connection.cursor()

    frequency_command = "SELECT COUNT(*), name " \
                        "FROM crystallization_chemicals " \
                        "GROUP BY chemical_name " \
                        "HAVING COUNT(*) > 9 " \
                        "ORDER BY 1 DESC"

    golden_list = []

    # Add special names
    special_names = ["k-po4", "amm po4", "nh4 formate", "gycine", "mg(oac)", "tric-cl", "ammonium sufate", "cdcl",
                     "phoshate", "pbs", "k2so4", "mg nitrate", "mgatp", "magnisium sulphate", "rbcl",
                     "ammoniun sulfate"]

    for name in special_names:
        golden_list.append(name)

    return golden_list


def query_criteria(connection, search_list, max_chemical_total):
    cursor = connection.cursor()

    count_command = "SELECT COUNT(*), parent_id FROM crystallization_chemicals GROUP BY parent_id " \
                    "ORDER BY 1 DESC"
    sort_command = "SELECT * FROM crystallization_chemicals ORDER BY parent_id"
    print_command = "SELECT * FROM entry_data ORDER BY id"

    entry_ids = []
    golden_list = create_list(connection)
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
    other_chemicals.append("mgatp")

    # First, check if it is already known that there are multiple chemicals associated with this entry
    for index, row in enumerate(cursor.execute(count_command)):
        chemical_count = row[0]
        entry_id = row[1]

        if chemical_count <= max_chemical_total:
            entry_ids.append(entry_id)

    # pprint("Length of entry_ids: {0}".format(len(entry_ids)))

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
                    if other_chemicals_search.group(0) == "mgatp":
                        pprint("Entry ID: {0}".format(entry_id))
                        pprint("We found ${0}$ in the details section!".format(other_chemicals_search.group(0)))
                else:
                    matching_entry_ids.append(entry_id)
        except IndexError:
            pprint("We've reached the end of the entry_ids list!")
            break

    # pprint("Length of matching_entry_ids: {0}".format(len(matching_entry_ids)))

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

    queried_chemical_name = search_list[0]

    pprint("There were a total of {0} matches to {1}.".format(len(good_matches), queried_chemical_name))

    filename = os.path.join(BASE, "{0}-matches.txt".format(queried_chemical_name))
    file = open(filename, 'w')
    file.write("{0} MATCHES \n\n".format(queried_chemical_name.upper()))
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
            pprint("We've reached the last element in the good_matches list!")
            break

    return good_matches
