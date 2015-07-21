__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

import os
from pprint import pprint

# CONSTANTS

BASE = "/home/vlad/Documents/Code/pdb-data-mining/Output/"

# FUNCTION DECLARATIONS

def export_concentration_data(connection, matches, name):
    cursor = connection.cursor()

    command = "SELECT * FROM crystallization_chemicals ORDER BY parent_id"

    filename_no_ext = os.path.join(BASE, name)
    filename = filename_no_ext + ".txt"
    file = open(filename, 'w')
    for index, row in enumerate(cursor.execute(command)):
        entry_id = row[1]
        concentration = row[2]
        converted_concentration = 0
        unit = row[3]
        conversion_occurred = False

        try:
            if entry_id in matches:
                if unit == "%":
                    converted_concentration = 4 * (concentration / 100)
                    conversion_occurred = True
                if unit == "mm":
                    converted_concentration = concentration / 1000
                    conversion_occurred = True
                # There is only one case of this, manually correct it (30.0%(v/v) in the PDBX_DETAILS section)
                if unit == "v/v":
                    converted_concentration = 4 * (30.0 / 100)
                    conversion_occurred = True

                if conversion_occurred:
                    concentration = converted_concentration

                file.write("{0},{1}\n"
                           .format(entry_id, concentration))

        except IndexError:
            pprint("We've reached the last element in the found_match list!")
            break


def export_coordinate_data(connection, matches, name):
    cursor = connection.cursor()

    command = "SELECT * FROM entry_coordinate_data ORDER BY parent_id"

    filename_no_ext = os.path.join(BASE, name)
    filename = filename_no_ext + ".txt"
    file = open(filename, 'w')
    for index, row in enumerate(cursor.execute(command)):
        entry_id = row[2]
        residual_id = row[4]
        atom_id = row[5]
        x_coordinate = row[6]
        y_coordinate = row[7]
        z_coordinate = row[8]

        try:
            if entry_id in matches:
                file.write("{0},{1},{2},{3},{4},{5}\n"
                           .format(entry_id, residual_id, atom_id, x_coordinate, y_coordinate, z_coordinate))

        except IndexError:
            pprint("We've reached the last element in the found_match list!")
            break
