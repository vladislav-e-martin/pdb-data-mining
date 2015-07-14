__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

import re
import uuid
from pprint import pprint

# CONSTANTS

SELECT_ENTRY_DETAILS = "SELECT * FROM entry_data ORDER BY id"

# FUNCTION DECLARATIONS

# Discover those entries that are properly delimited with a comma or semicolon
def discover_common_delimited_entries(connection):
    cursor = connection.cursor()

    # Store the relevant entries with properly delimited details sections
    delimited_entries = []

    for index, row in enumerate(cursor.execute(SELECT_ENTRY_DETAILS)):
        # Store the entry id of the current structure's entry
        entry_id = row[0]
        # Store the details section of the current structure's entry
        details = row[6]
        # Find only details sections that contain one of these common delimiters
        # (not usually found in chemical formulae or names)
        find_delimiters = re.findall('[,;]*', details)
        confirmed = False

        for delimiter in find_delimiters:

            if delimiter is not None and confirmed is not True:
                delimited_entries.append(entry_id)
                confirmed = True

    return delimited_entries


# Need to keep track of the associated entry_id as well
def create_golden_reference_list(connection, good_entries):
    cursor = connection.cursor()

    # Store the chemical concentrations of the relevant entries
    concentration_index = 0
    chemical_concentrations = []

    # Store the chemical names of the relevant entries
    name_index = 0
    chemical_names = []

    # Store the entry ids of the relevant entries
    chemical_ids = []

    entry_index = 0
    for index, row in enumerate(cursor.execute(SELECT_ENTRY_DETAILS)):
        entry_id = row[0]
        details = row[6].lower()

        comma_delimited_remarks = re.split('(?:[,]|[;])', details)

        # Check if this structure's entry is delimited properly
        if entry_id == good_entries[entry_index]:

            # Find each chemical name used in the structure's crystallization
            for comma_delimited_name in comma_delimited_remarks:
                english_delimited_names = re.split('(?:and)', comma_delimited_name)

                # Check if the chemical name is delimited by an english word
                for english_delimited_name in english_delimited_names:
                    # Period is not a delimiter when followed by a number, but rather part of a concentration value
                    period_delimited_names = re.split('(?:[.][^0-9]+)', english_delimited_name)

                    # Check if the chemical name is delimited with a period
                    for period_delimited_name in period_delimited_names:

                        # Check if some sort of concentration unit (or if is PEG) is provided
                        molarity = re.search('((\d+|\d+[.]\d+)\s*m{1,2}\s+)', period_delimited_name)
                        percent = re.search('((\d+|\d+[.]\d+)\s*[%])', period_delimited_name)
                        peg = re.search('(peg)', period_delimited_name)

                        # If no form of concentration is provided, the phrase is useless
                        if molarity is None and percent is None and peg is None:
                            # Skip this iteration of the loop so that this name is not stored in the list
                            continue

                        # Split concentration values and units from their associated chemical names
                        concentration_delimited_names = re.split('(\d+\s*m{1,2}\s+|\d+[.]\d+\s*m{1,2}\s+|'
                                                                 '\d+\s*[%]|\d+[.]\d+\s*[%]|'
                                                                 '[(]*v/v[)]*|[(]*w/v[)]*|[(]*w/w[)]*\s*)',
                                                                 period_delimited_name)

                        for concentration_delimited_name in concentration_delimited_names:
                            new_name = concentration_delimited_name

                            # Continue applying filters until all have been fulfilled
                            while True:
                                ph_filtered = False
                                while True:
                                    # All of the various fashions a pH value is denoted
                                    ph = re.search('(ph\s*(\d+|\d+[.]\d+)|'
                                                   'ph\s*[=]\s*(\d+|\d+[.]\d+)|'
                                                   'ph\s*[-]\s*(\d+|\d+[.]\d+)|'
                                                   'ph\s*[(]\s*(\d+|\d+[.]\d+))', new_name)
                                    # Check if a pH value is contained in the chemical's name
                                    if ph is not None:
                                        ph_filter = re.sub('(ph).*', '', new_name)
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
                                        paren_filter = re.sub('([(].*|[)].*)', '', new_name)
                                        new_name = paren_filter
                                        paren_filtered = True
                                    else:
                                        # If this is not the case, make sure the name remains consistent regardless
                                        paren_filter = new_name
                                        break

                                grammar_filtered = False
                                while True:
                                    grammar = re.search('\s+(with|at|in|of|was|were|for|against|to)\s+',
                                                        paren_filter)
                                    words = re.search('(buffer|inhibitor|compound|protein|reservoir|saturated|'
                                                      'solution)',
                                                      paren_filter)
                                    # Check if an English word is contained in the chemical's name
                                    if grammar is not None or words is not None:
                                        grammar_filter = re.sub('\s*(with.*|at.*|in.*|of.*|was.*|were.*|for.*|'
                                                                'against.*|to.*|buffer.*|inhibitor.*|compound.*|'
                                                                'protein.*|reservoir.*|saturated.*|solution.*)\s*',
                                                                '', new_name)
                                        new_name = grammar_filter
                                        grammar_filtered = True
                                    else:
                                        grammar_filter = new_name
                                        break

                                new_name = grammar_filter

                                # Filters have been applied completely by this point, break the loop
                                if not ph_filtered and not paren_filtered and not grammar_filtered:
                                    break

                            # The final chemical name is equivalent to the results of the last filter applied
                            concentration_check = re.search('((\d+|\d+[.]\d+)\s*m{1,2}\s+)', new_name)
                            percent_check = re.search('((\d+|\d+[.]\d+)\s*[%])', new_name)
                            w_or_v_check = re.search('([(]*v/v[)]*|[(]*w/v[)]*|[(]*w/w[)]*)', new_name)

                            if concentration_check is not None or percent_check is not None or w_or_v_check is not None:
                                chemical_concentration = new_name.strip()
                                chemical_concentrations.append(chemical_concentration)
                                concentration_index += 1
                                # pprint("{0} is a concentration! Storing the string.".format(chemical_concentration))
                                continue

                            if concentration_index == (name_index + 1):
                                chemical_name = new_name.strip()

                                if "2-methylpentane-2" in chemical_name or "2-methyl-2" in chemical_name:
                                    chemical_name = "2-methylpentane-2,4-diol"

                                if chemical_name == "eg":
                                    chemical_name = "ethylene glycol"

                                chemical_names.append(chemical_name)
                                chemical_ids.append(entry_id)
                                name_index += 1
                                # pprint("{0} is a name! Storing the string and it's ID.".format(chemical_name))
            entry_index += 1

    return chemical_names, chemical_concentrations, chemical_ids


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


def refine_golden_reference_list(data):
    names = data[0]
    concentrations = data[1]
    ids = data[2]

    refined_names = []
    refined_concentrations = []
    refined_ids = []

    for index, name in enumerate(names):
        # Remove any items that CANNOT be chemical names (i.e., spaces, only numbers, anything with an equal sign in it)
        if is_name_short(name) or is_name_numeric(name) or is_name_not_feasible(name):
            pprint("Removing ${0}$ from the golden list of valid chemical names.".format(name))
        # Remove all information related to that chemical entry
        else:
            refined_names.append(name)
            refined_concentrations.append(concentrations[index])
            refined_ids.append(ids[index])

    return refined_names, refined_concentrations, refined_ids


def finalize_golden_reference_list(data):
    names = data[0]
    concentrations = data[1]
    ids = data[2]

    rows = len(concentrations)
    data_to_store = [[0 for x in range(5)] for x in range(rows)]
    data_index = 0

    for index, full_concentration in enumerate(concentrations):
        concentration_search = re.findall('(\d+|\d+[.]\d+)\s*(m{1,2}|[%])\s*', full_concentration)
        w_v_search = re.findall('((v/v)|(vol/vol)|(w/v)|(wt/vol)|(w/w)|(wt/wt))\s*', full_concentration)

        for concentration in concentration_search:
            value = concentration[0]
            unit = concentration[1]
            data_to_store[data_index][0] = str(uuid.uuid4())
            data_to_store[data_index][1] = ids[index]
            data_to_store[data_index][2] = value
            data_to_store[data_index][3] = unit
            data_to_store[data_index][4] = names[index]
            data_index += 1

        for w_v in w_v_search:
            value = 0
            unit = w_v[0]
            data_to_store[data_index][0] = str(uuid.uuid4())
            data_to_store[data_index][1] = ids[index]
            data_to_store[data_index][2] = value
            data_to_store[data_index][3] = unit
            data_to_store[data_index][4] = names[index]
            data_index += 1

    return data_to_store
