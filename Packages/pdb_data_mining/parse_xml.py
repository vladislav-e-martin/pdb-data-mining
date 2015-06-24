# LIBRARIES AND PACKAGES

import xml.etree.ElementTree as ET
import os
from pprint import pprint

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


def extract_value(root, query: str, namespace: str, converter) -> str or float or int:
    value = 0
    for element in root.findall(query, namespace):
        if element is not None:
            value = converter(element.text)
    return value

# Fill a list with the criteria each structure must meet to be considered for storage in the database
def fill_raw(files, columns) -> object:
    rows = len(files)
    data = [[0 for x in range(columns)] for x in range(rows)]
    for source_index, file in enumerate(files):
        try:
            pprint("Current source_index: {0}".format(source_index))
            tree = ET.parse(file)

            root = tree.getroot()
            # Make XPATHs simpler
            queries = [".",
                       "./PDBx:refine_histCategory/PDBx:refine_hist/PDBx:pdbx_number_atoms_protein",
                       "./PDBx:reflnsCategory/PDBx:reflns/PDBx:d_resolution_high",
                       "./PDBx:exptlCategory/PDBx:exptl",
                       "./PDBx:exptl_crystalCategory/PDBx:exptl_crystal/PDBx:density_Matthews",
                       "./PDBx:exptl_crystalCategory/PDBx:exptl_crystal/PDBx:density_percent_sol",
                       "./PDBx:exptl_crystal_growCategory/PDBx:exptl_crystal_grow/PDBx:pH",
                       "./PDBx:exptl_crystal_growCategory/PDBx:exptl_crystal_grow/PDBx:temp",
                       "./PDBx:exptl_crystal_growCategory/PDBx:exptl_crystal_grow/PDBx:pdbx_details"]
            namespace = {'PDBx': 'http://pdbml.pdb.org/schema/pdbx-v40.xsd'}

            current_col = 0

            # Entry ID
            for element in root.findall(queries[0], namespace):
                # Remove the last seven letters from this string (i.e., the "-noatom" part)
                entry_id = element.get('datablockName')[:-7]
                pprint("The entry ID of this structure: {0}".format(entry_id))
                data[source_index][current_col] = entry_id
                current_col += 1


            # Number of protein atoms
            count = extract_value(root, queries[1], namespace, int)
            data[source_index][current_col] = count
            current_col += 1

            # Resolution of structure
            resolution = extract_value(root, queries[2], namespace, float)
            data[source_index][current_col] = resolution
            current_col += 1

            # Method used to analyze structure
            for element in root.findall(queries[3], namespace):
                method = element.get('method')
                data[source_index][current_col] = method
                current_col += 1

            # Matthew's density
            matthews_density = extract_value(root, queries[4], namespace, float)
            data[source_index][current_col] = matthews_density
            current_col += 1

            # Percent solvent density
            percent_density = extract_value(root, queries[5], namespace, float)
            data[source_index][current_col] = percent_density
            current_col += 1

            # pH of solution
            pH = extract_value(root, queries[6], namespace, float)
            data[source_index][current_col] = pH
            current_col += 1

            # Temperature of solution
            temp = extract_value(root, queries[7], namespace, float)
            data[source_index][current_col] = temp
            current_col += 1

            found = False
            # Details of the crystallization conditions for the structure
            details = extract_value(root, queries[8], namespace, str)
            if not found:
                data[source_index][current_col] = details
                found = True
            else:
                pass
        except ET.ParseError:
            pprint("This file could not be parsed. Deleting the file so it may be downloaded again...")
            os.remove(file)
            pass
    return data

# Sort the structures that do not meet the criteria from the list
def filter_indices(raw) -> object:
    indices_to_keep = []

    # Criteria for sorting purposes and their associated column numbers in raw[][]
    a_i = 1
    r_i = 2
    resolution = 2.50
    m_i = 3
    method = "X-RAY DIFFRACTION"

    for source_index in range(len(raw)):
        try:
            # Store the source_index of the rows that do meet the criteria (The result of this is the overlap of those that
            # have structures and those that meet the criteria)
            if raw[source_index][a_i] > 0 and raw[source_index][r_i] <= resolution and raw[source_index][
                m_i] == method and \
                            raw[source_index][4] != 0 and raw[source_index][5] != 0 and raw[source_index][8] != "0":
                indices_to_keep.append(source_index)
                pprint("The source_index {0} is worth noting!".format(source_index))
            # Ignore the rows if they do not meet the criteria
            else:
                pprint("This structure does not meet the criteria.")
        except TypeError:
            pprint("One of the criteria was not provided with the structure. This structure will be ignored.")
            pass
    pprint("There were {0} indices worth noting!".format(len(indices_to_keep)))
    return indices_to_keep

# Fill a list with the important information stored in the structures that have will be stored in the database
def fill_sorted(raw, indices, columns):
    rows = len(indices)
    refined_data = [[0 for x in range(columns)] for x in range(rows)]

    # Current source_index of the new sorted data[][]
    target_index = 0
    for source_index in range(len(raw)):
        try:
            if source_index == indices[target_index]:
                refined_data[target_index][0] = raw[source_index][0]
                refined_data[target_index][1] = raw[source_index][2]
                refined_data[target_index][2] = raw[source_index][4]
                refined_data[target_index][3] = raw[source_index][5]
                refined_data[target_index][4] = raw[source_index][6]
                refined_data[target_index][5] = raw[source_index][7]
                refined_data[target_index][6] = raw[source_index][8]
                target_index += 1
            else:
                pass
        except IndexError:
            pprint("We have reached the end of the indices[] list. All the extracted information has been stored!")
            break
    return refined_data
