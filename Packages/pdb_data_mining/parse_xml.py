# LIBRARIES AND PACKAGES

import xml.etree.ElementTree as ET
import os
from pprint import pprint
import sys

# FUNCTION DECLARATIONS

# Fill a list with the filepaths to all downloaded structures
def fill_list(base):
    xml_files = []
    for root, directories, files in os.walk(base):
        pprint("Root: {0} Directories: {1} Files: {2}".format(root, len(directories), len(files)))
        for name in files:
            filepath = os.path.join(root, name)
            # Check if the compressed file still exists
            if name.endswith(".gz"):
                pprint("The compressed " + name + " is now being removed.")
                # Delete the compressed file to save space on the hard drive
                os.remove(filepath)
            else:
                # Add the XML file to the list of XML files 
                xml_files.append(filepath)
    return xml_files


# Fill a list with the criteria each structure must meet to be considered for storage in the database
def fill_raw(files, columns):
    rows = len(files)
    data = [[0 for x in range(columns)] for x in range(rows)]

    for index, file in enumerate(files):
        try:
            pprint("Current index: {0}".format(index))
            tree = ET.parse(file)

            root = tree.getroot()
            # Make XPATHs simpler
            namespace = {'PDBx': 'http://pdbml.pdb.org/schema/pdbx-v40.xsd'}

            current_col = 0

            # Number of protein atoms
            for element in root.findall("./PDBx:refine_histCategory/PDBx:refine_hist/PDBx:pdbx_number_atoms_protein",
                                        namespace):
                count = int(element.text)
                data[index][current_col] = count
                current_col += 1

            # Resolution of structure
            for element in root.findall("./PDBx:reflnsCategory/PDBx:reflns/PDBx:d_resolution_high", namespace):
                resolution = float(element.text)
                data[index][current_col] = resolution
                current_col += 1

            # Method used to analyze structure
            for element in root.findall("./PDBx:exptlCategory/PDBx:exptl", namespace):
                method = element.get('method')
                data[index][current_col] = method
                current_col += 1

            # Details of the crystallization conditions for the structure
            for element in root.findall("./PDBx:exptl_crystal_growCategory/PDBx:exptl_crystal_grow/PDBx:pdbx_details",
                                        namespace):
                data[index][current_col] = element.text
                current_col += 1
        except ET.ParseError:
            pprint("This file could not be parsed. Deleting the file so it may be downloaded again...")
            os.remove(file)
            pass

    return data


# Sort the structures that do not meet the criteria from the list
def filter_indices(unsorted, columns):
    indices = []

    # CRITERIA
    # Atom index
    a_i = 0
    # Resolution index
    r_i = 1
    resolution = 2.50
    # Method index
    m_i = 2
    method = "X-RAY DIFFRACTION"

    # Check the rows against the specified criteria
    for index in range(len(unsorted)):
        try:
            # Store the index of the rows that do meet the criteria
            if unsorted[index][a_i] > 0 and unsorted[index][r_i] <= resolution and unsorted[index][m_i] == method:
                indices.append(index)
                pprint("The index {0} is worth noting!".format(index))
            # Ignore the rows if they do not meet the criteria
            else:
                pprint("This structure does not meet the criteria.")
        except TypeError:
            pprint("One of the criteria was not provided with the structure. This structure will be ignored.")
            pass

    return indices

# Fill a list with the important information stored in the structures that have will be stored in the database
def fill_sorted(files, indices, columns):
    rows = len(indices)
    data = [[0 for x in range(columns)] for x in range(rows)]

    current_index = 0
    for index, file in enumerate(files):
        if index == indices[current_index]:
            try:
                pprint("Current index: {0}".format(current_index))
                tree = ET.parse(file)

                root = tree.getroot()
                # Make XPATHs simpler
                namespace = {'PDBx': 'http://pdbml.pdb.org/schema/pdbx-v40.xsd'}

                current_col = 0

                # Entry ID of the structure
                for element in root.findall("."):
                    id = element.get('datablockName')
                    data[current_index][current_col] = id
                    pprint(data[current_index][current_col])
                    current_col += 1

                # Resolution of the structure
                for element in root.findall("./PDBx:reflnsCategory/PDBx:reflns/PDBx:d_resolution_high", namespace):
                    resolution = float(element.text)
                    data[current_index][current_col] = resolution
                    pprint(data[current_index][current_col])
                    current_col += 1
                current_index += 1
            except ET.ParseError:
                pprint("This file could not be parsed. Deleting the file so it may be downloaded again...")
                os.remove(file)
                pass
        else:
            pass

    return data

# Fill a list with only the crystallization information of each structure that will be stored in the database
def fill_details(files, indices):
    data = []

    current_index = 0
    for index, file in enumerate(files):
        if index == indices[current_index]:
            try:
                pprint("Current index: {0}".format(current_index))
                tree = ET.parse(file)

                root = tree.getroot()
                # Make XPATHs simpler
                namespace = {'PDBx': 'http://pdbml.pdb.org/schema/pdbx-v40.xsd'}

                # Resolution of the structure
                for element in root.findall("./PDBx:exptl_crystal_growCategory/PDBx:exptl_crystal_grow/PDBx:pdbx_details",
                                        namespace):
                    data.append(element.text)
                    pprint(len(data))
                current_index += 1
            except ET.ParseError:
                pprint("This file could not be parsed. Deleting the file so it may be downloaded again...")
                os.remove(file)
                pass
        else:
            pass

    return data