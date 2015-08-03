__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

import re
import uuid
from pprint import pprint
from Packages.pdb_data_mining import manage_sql as sql

# CONSTANTS

SELECT_ENTRY_DETAILS = "SELECT id, details FROM entry_data ORDER BY id"

aliases = {"2-methylpentane-2": "2-methylpentane-2,4-diol",
           "2-methyl-2": "2-methylpentane-2,4-diol",
           "eg": "ethylene glycol",
           "etgly": "ethylene glycol"}

# FUNCTION DECLARATIONS

# Discover those entries that are properly delimited with a comma or semicolon
def discover_interpretable_entries(connection):
    cursor = connection.cursor()

    # Store the relevant entries with properly delimited details sections
    interpretable_entries = []

    for row in cursor.execute(SELECT_ENTRY_DETAILS).fetchall():
        # Store the entry id of the current structure's entry
        entry_id = row[0]
        # Store the details section of the current structure's entry
        details = row[1].lower()

        interpretable_concentration = re.search('(\d+\s+m{1,2}|\d+[.]\d+\s+m{1,2}|'
                                                '\d+\s*[%]|\d+[.]\d+\s*[%])', details)
        delimited_details = re.search('[^0-9]+[,][^0-9]+|[;]|(and)|[.][^0-9]+', details)

        if interpretable_concentration is not None and delimited_details is not None:
            # print("A delimited set of values was discovered in the below details tag.")
            # pprint("Entry ID: {0}".format(entry_id))
            # pprint(details)
            # print("\n")
            interpretable_entries.append(entry_id)

    pprint("We have found a total of {0} interpretable crystallization conditions.".format(len(interpretable_entries)))

    return interpretable_entries


# Need to keep track of the associated entry_id as well
def parse_crystallization_chemicals(connection, good_entries):
    cursor = connection.cursor()

    sql.delete_all_rows(connection, "crystallization_chemicals")
    sql.delete_table(connection, "crystallization_chemicals")

    # Store all chemical concentrations containing an identifiable concentration and unit pair
    concentrations = []

    # Store all chemical names
    names = []

    entry_ids = []

    concentration_found = False

    for row in cursor.execute(SELECT_ENTRY_DETAILS).fetchall():
        entry_id = row[0]
        details = row[1].lower()

        # Check if this structure's entry is delimited properly
        for entry in good_entries:

            if entry_id == entry:

                delimited_remarks = re.split('(?:[,][^0-9]+|[;]|[.][^0-9]+)', details)

                # Find each chemical name used in the structure's crystallization
                for delimited_remark in delimited_remarks:
                    english_delimited_remarks = re.split('(?:and)', delimited_remark)

                    # Check if the chemical name is delimited by an english word
                    for english_delimited_remark in english_delimited_remarks:
                        trimmed_remark = english_delimited_remark.strip()

                        # Find each concentration, unit, and chemical name grouping
                        concentration_remarks = re.split('(\d+\s*m{1,2}\s+|'
                                                         '\d+[.]\d+\s*m{1,2}\s+|'
                                                         '\d+\s*[%]\s+|'
                                                         '\d+[.]\d+\s*[%]\s+)', trimmed_remark)

                        for concentration_remark in [remark for remark in concentration_remarks if remark]:
                            remark = concentration_remark

                            # Check if at least one concentration value and unit pair is provided
                            is_concentration = re.search('(\d+\s*m{1,2}\s+|'
                                                         '\d+[.]\d+\s*m{1,2}\s+|'
                                                         '\d+\s*[%]\s+|'
                                                         '\d+[.]\d+\s*[%]\s+)', remark)

                            if is_concentration is None and not concentration_found:
                                concentration_found = False
                                continue
                            elif is_concentration is not None and not concentration_found:
                                concentration = remark.strip()
                                # print("A valid chemical concentration and unit pair was discovered in {0}, "
                                #       "it is displayed below.".format(entry_id))
                                # pprint(concentration)
                                # pprint("Storing the chemical concentration and unit pair...")
                                # print("\n")
                                concentrations.append(concentration)
                                entry_ids.append(entry_id)
                                concentration_found = True
                                continue

                            # Continue applying filters until all have been fulfilled
                            while True:
                                # pprint("Filtering {0}".format(remark))

                                ph_filtered = False
                                while True:
                                    # All of the various fashions a pH value is denoted
                                    ph = re.search('(ph\s*(\d+|\d+[.]\d+)|'
                                                   'ph\s*[=]\s*(\d+|\d+[.]\d+)|'
                                                   'ph\s*[-]\s*(\d+|\d+[.]\d+)|'
                                                   'ph\s*[(]\s*(\d+|\d+[.]\d+)|'
                                                   'ph\s*[:]\s*(\d+|\d+[.]\d+)|'
                                                   '\d+[.]\d+)', remark)
                                    # Check if a pH value is contained in the chemical's name
                                    if ph is not None:
                                        # pprint("Found a pH match!")
                                        ph_filter = re.sub('(ph\s*(\d+|\d+[.]\d+)|'
                                                           'ph\s*[=]\s*(\d+|\d+[.]\d+)|'
                                                           'ph\s*[-]\s*(\d+|\d+[.]\d+)|'
                                                           'ph\s*[(]\s*(\d+|\d+[.]\d+)|'
                                                           'ph\s*[:]\s*(\d+|\d+[.]\d+)|'
                                                           '\d+[.]\d+).*', '', remark)
                                        remark = ph_filter
                                        ph_filtered = True
                                    else:
                                        break

                                special_filtered = False
                                while True:
                                    # All of the various special characters observed in potential chemical names
                                    special = re.search('\s*([,]$|[=]|[.]$|\n|[/]|[-]$|\s+\d+$)', remark)

                                    # Check if a special character is contained in the chemical's name
                                    if special is not None:
                                        special_filter = re.sub('([,]$|[=]|[.]$|\n|[/]|[-]$|\s+\d+$).*', '', remark)
                                        remark = special_filter
                                        special_filtered = True
                                    else:
                                        break

                                paren_filtered = False
                                while True:
                                    open_parentheses = re.search('([(])', remark)
                                    close_parentheses = re.search('([)])', remark)
                                    # Check if only one parentheses is contained in the chemical's name
                                    if open_parentheses is not None and close_parentheses is None or \
                                                            close_parentheses is not None and open_parentheses is None:
                                        paren_filter = re.sub('([(]|[)]).*', '', remark)
                                        remark = paren_filter
                                        paren_filtered = True
                                    else:
                                        break

                                bracket_filtered = False
                                while True:
                                    open_bracket = re.search('([\[])', remark)
                                    close_bracket = re.search('([\]])', remark)
                                    # Check if only one parentheses is contained in the chemical's name
                                    if open_bracket is not None and close_bracket is None or \
                                                            close_bracket is not None and open_bracket is None:
                                        bracket_filter = re.sub('([\[]|[\]]).*', '', remark)
                                        remark = bracket_filter
                                        bracket_filtered = True
                                    else:
                                        break

                                grammar_filtered = False
                                while True:
                                    grammar = re.search('\s+(with|at|in|of|was|were|for|against|to)\s+', remark)
                                    words = re.search('(buffer|inhibitor|compound|protein|reservoir|saturated|'
                                                      'solution|prior|cryo|equilib|mixed|until|under|using|'
                                                      'well)', remark)
                                    # Check if an English word is contained in the chemical's name
                                    if grammar is not None:
                                        grammar_filter = re.sub('\s*(with|at|in|of|was|were|for|'
                                                                'against|to)\s*.*', '', remark)
                                        remark = grammar_filter
                                        grammar_filtered = True
                                    elif words is not None:
                                        word_filter = re.sub('\s*(buffer|inhibitor|compound|'
                                                             'protein|reservoir|saturated|solution|'
                                                             'prior|cryo|equilib|mixed|until|under|'
                                                             'using|well)\s*.*', '', remark)
                                        remark = word_filter
                                        grammar_filtered = True
                                    else:
                                        break

                                wv_filtered = False
                                while True:
                                    wv_parentheses = re.search('[(]((v/\s*v)|(vol/\s*vol)|(w/\s*v)|(wt/\s*vol)|'
                                                               '(v/\s*w)|(vol/\s*wt)|(w/\s*w)|(wt/\s*wt))[)]', remark)
                                    wv_plain = re.search('((v/\s*v)|(vol/\s*vol)|(w/\s*v)|(wt/\s*vol)|'
                                                         '(v/\s*w)|(vol/\s*wt)|(w/\s*w)|(wt/\s*wt))', remark)
                                    if wv_parentheses is not None:
                                        wv_paren_filter = re.sub('[(]((v/\s*v)|(vol/\s*vol)|(w/\s*v)|(wt/\s*vol)|'
                                                                 '(v/\s*w)|(vol/\s*wt)|(w/\s*w)|(wt/\s*wt))[)]', '',
                                                                 remark)
                                        remark = wv_paren_filter
                                        wv_filtered = True
                                    elif wv_plain is not None:
                                        wv_plain_filter = re.sub('((v/\s*v)|(vol/\s*vol)|(w/\s*v)|(wt/\s*vol)|'
                                                                 '(v/\s*w)|(vol/\s*wt)|(w/\s*w)|(wt/\s*wt))', '',
                                                                 remark)
                                        remark = wv_plain_filter
                                        wv_filtered = True
                                    else:
                                        break

                                # Filters have been applied completely by this point, break the loop
                                if not ph_filtered and not paren_filtered and not grammar_filtered and not \
                                        bracket_filtered and not special_filtered and not wv_filtered:
                                    break

                            if concentration_found:
                                name = remark.strip()
                                # print("A valid chemical name was discovered in {0}, "
                                #       "it is displayed below.".format(entry_id))
                                # pprint(name)
                                # pprint("Storing the chemical name...")
                                # print("\n")
                                names.append(name)
                                concentration_found = False

    pprint("Length: {0} and Length {1}".format(len(concentrations), len(names)))
    sql.create_crystallization_chemicals_table(connection)

    for index, pair in enumerate(concentrations):
        concentration_search = re.findall('(\d+|\d+[.]\d+)\s*(m{1,2}|[%])\s*', pair)

        for full_concentration in concentration_search:
            data_to_store = []
            if not names[index].isdigit() and names[index] and len(names[index]) > 1:
                unique_id = str(uuid.uuid4())
                value = full_concentration[0]
                unit = full_concentration[1]
                # pprint("ID: {0}".format(entry_ids[index]))
                # pprint("Value: {0} Unit: {1}".format(value, unit))
                # pprint("Name: {0}".format(names[index]))
                data_to_store.append(unique_id)
                data_to_store.append(entry_ids[index])
                data_to_store.append(value)
                data_to_store.append(unit)
                data_to_store.append(names[index])

                # Insert each row of values from data_to_store
                sql.add_crystallization_chemicals_row(connection, (data_to_store,))

    # Commit changes to the database
    sql.commit_changes(connection)
