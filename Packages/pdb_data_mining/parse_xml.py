# LIBRARIES AND PACKAGES

import xml.etree.ElementTree as et
import os
import uuid
from pprint import pprint

# FUNCTION DECLARATIONS

# Fill a list with the file-paths to all downloaded structures
def list_files(base, entry_ids):
    xml_files = []
    for root, directories, files in os.walk(base):
        # pprint("Root: {0} Directories: {1} Files: {2}".format(root, len(directories), len(files)))
        for name in files:
            filepath = os.path.join(root, name)
            filename = name[:-4]
            pprint("The name of the file: {0}".format(filename))
            # Check if the compressed file still exists
            if name.endswith(".gz"):
                pprint("The compressed " + name + " is now being removed.")
                # Delete the compressed file to save space on the hard drive
                os.remove(filepath)
            if filename in entry_ids:
                # Add the XML file to the list of XML files
                xml_files.append(filepath)
    return xml_files


def extract_value(root, query: str, namespace: dict, converter) -> str or float or int:
    value = 0
    found = False
    for element in root.findall(query, namespace):
        if not found:
            if element.text is not None:
                value = converter(element.text)
                found = True
        else:
            pass
    return value


# Fill a list with the criteria each structure must meet to be considered for storage in the database
def fill_raw(files, columns) -> object:
    rows = len(files)
    data = [[0 for x in range(columns)] for x in range(rows)]
    for source_index, file in enumerate(files):
        try:
            pprint("The current index: {0}".format(source_index))
            tree = et.parse(file)

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
                # pprint("The entry ID of this structure: {0}".format(entry_id))
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

            found = False
            # Method used to analyze structure
            for element in root.findall(queries[3], namespace):
                if not found:
                    method = element.get('method')
                    data[source_index][current_col] = method
                    current_col += 1
                    found = True
                else:
                    pass

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
        except et.ParseError:
            # pprint("This file could not be parsed. Deleting the file so it may be downloaded again...")
            os.remove(file)
            pass
    return data


def fill_raw_coordinates(files: list, columns: int, ionic: int) -> object:
    relevant_residual_ids = ["ASP", "ARG", "GLU", "LYS"]
    relevant_atom_ids = ["CA", "OD1", "OD2", "NH1", "NH2", "OE1", "OE2", "NZ"]

    entry_ids = []
    first_chain_ids = []
    chain_ids = []
    group_ids = []
    sequence_ids = []
    residual_ids = []
    atom_ids = []
    x_coordinates = []
    y_coordinates = []
    z_coordinates = []

    entry_ids_to_keep = []
    sequence_ids_to_keep = []
    residual_ids_to_keep = []
    atom_ids_to_keep = []
    x_coordinates_to_keep = []
    y_coordinates_to_keep = []
    z_coordinates_to_keep = []

    for source_index, file in enumerate(files):
        try:
            filename = os.path.basename(file)
            entry_id = filename[:-4]

            pprint("The current index: {0}".format(source_index))
            pprint("The current entry ID: {0}".format(entry_id))
            tree = et.parse(file)

            root = tree.getroot()
            # Make XPATHs simpler
            queries = ["./PDBx:atom_siteCategory/PDBx:atom_site/PDBx:auth_asym_id",
                       "./PDBx:atom_siteCategory/PDBx:atom_site/PDBx:group_PDB",
                       "./PDBx:atom_siteCategory/PDBx:atom_site/PDBx:auth_comp_id",
                       "./PDBx:atom_siteCategory/PDBx:atom_site/PDBx:auth_atom_id",
                       "./PDBx:atom_siteCategory/PDBx:atom_site/PDBx:auth_seq_id",
                       "./PDBx:atom_siteCategory/PDBx:atom_site/PDBx:Cartn_x",
                       "./PDBx:atom_siteCategory/PDBx:atom_site/PDBx:Cartn_y",
                       "./PDBx:atom_siteCategory/PDBx:atom_site/PDBx:Cartn_z"]
            namespace = {'PDBx': 'http://pdbml.pdb.org/schema/pdbx-v40.xsd'}

            first_chain_element = root.find(queries[0], namespace)
            first_chain = first_chain_element.text

            # Chain ID of this row of elements
            for element in root.findall(queries[0], namespace):
                chain_id = element.text
                chain_ids.append(chain_id)
                first_chain_ids.append(first_chain)
                entry_ids.append(entry_id)

            # Group ID of this row of elements
            for element in root.findall(queries[1], namespace):
                group_id = element.text
                group_ids.append(group_id)

            # Residual ID of this row of elements
            for element in root.findall(queries[2], namespace):
                residual_id = element.text
                residual_ids.append(residual_id)

            # Atom ID of this row of elements
            for element in root.findall(queries[3], namespace):
                atom_id = element.text
                atom_ids.append(atom_id)

            # Sequence ID of this row of elements
            for element in root.findall(queries[4], namespace):
                sequence_id = element.text
                sequence_ids.append(sequence_id)

            # X-coordinate of this atom
            for element in root.findall(queries[5], namespace):
                x_coordinate = float(element.text)
                x_coordinates.append(x_coordinate)

            # Y-coordinate of this atom
            for element in root.findall(queries[6], namespace):
                y_coordinate = float(element.text)
                y_coordinates.append(y_coordinate)

            # Z-coordinate of this atom
            for element in root.findall(queries[7], namespace):
                z_coordinate = float(element.text)
                z_coordinates.append(z_coordinate)

        except et.ParseError:
            pprint("This file could not be parsed. Deleting the file so it may be downloaded again...")
            os.remove(file)
            pass

    # pprint("Number of atom elements: {0}".format(len(atom_ids)))

    for index, value in enumerate(chain_ids):
        if chain_ids[index] == first_chain_ids[index] and group_ids[index] == "ATOM" and \
                        residual_ids[index] in relevant_residual_ids and atom_ids[index] in relevant_atom_ids:
            entry_ids_to_keep.append(entry_ids[index])
            sequence_ids_to_keep.append(sequence_ids[index])
            residual_ids_to_keep.append(residual_ids[index])
            atom_ids_to_keep.append(atom_ids[index])
            x_coordinates_to_keep.append(x_coordinates[index])
            y_coordinates_to_keep.append(y_coordinates[index])
            z_coordinates_to_keep.append(z_coordinates[index])

    # pprint("Number of atom elements kept: {0}".format(len(atom_ids_to_keep)))

    rows = len(entry_ids_to_keep)
    data = [[0 for x in range(columns)] for x in range(rows)]

    # Fill the list
    for index, value in enumerate(entry_ids_to_keep):
        data[index][0] = str(uuid.uuid4())
        data[index][1] = ionic
        data[index][2] = entry_ids_to_keep[index].upper()
        pprint(data[index][2])
        data[index][3] = sequence_ids_to_keep[index]
        data[index][4] = residual_ids_to_keep[index]
        data[index][5] = atom_ids_to_keep[index]
        data[index][6] = x_coordinates_to_keep[index]
        data[index][7] = y_coordinates_to_keep[index]
        data[index][8] = z_coordinates_to_keep[index]

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
            if raw[source_index][a_i] > 0 and raw[source_index][r_i] <= resolution and raw[source_index][m_i] == method \
                    and raw[source_index][4] != 0 and raw[source_index][5] != 0 and raw[source_index][6] != 0 and \
                            raw[source_index][7] != 0 and raw[source_index][8] != "0":
                indices_to_keep.append(source_index)
                pprint("The source_index {0} is worth noting!".format(source_index))
            # Ignore the rows if they do not meet the criteria
            else:
                # pprint("This structure does not meet the criteria.")
                pass
        except TypeError:
            # pprint("One of the criteria was not provided with the structure. This structure will be ignored.")
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
