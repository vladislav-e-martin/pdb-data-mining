# LIBRARIES AND PACKAGES

from Packages.pdb_data_mining import manage_sql as sql
import xml.etree.cElementTree as etree
import os
import uuid
from pprint import pprint

# FUNCTION DECLARATIONS

# Discover file names of every structure stored in the local PDB directory
def discover_file_names(base):
    xml_files = []
    for root, directories, files in os.walk(base):
        # pprint("Root: {0} Directories: {1} Files: {2}".format(root, len(directories), len(files)))
        for name in files:
            file_path = os.path.join(root, name)
            if name.endswith(".xml"):
                xml_files.append(file_path)

    pprint("We have downloaded a total of {0} structures from the PDB as of right now.".format(len(xml_files)))

    return xml_files


# Extract information for filtering structures based on existence of protein atoms, resolution, experimental method,
#  and existence of crystallization information
def extract_data(files, db_connection, min_resolution, exptl_method) -> object:
    for source_index, file in enumerate(files):
        data = []
        protein_count = 0
        resolution = 0
        method = ""
        try:
            pprint("The current index: {0}".format(source_index))

            tree = etree.parse(file)

            root = tree.getroot()
            # Make XPATHs simpler
            queries = [".",
                       "./PDBx:refine_histCategory/PDBx:refine_hist/PDBx:pdbx_number_atoms_protein",
                       "./PDBx:reflnsCategory/PDBx:reflns/PDBx:d_resolution_high",
                       "./PDBx:exptlCategory/PDBx:exptl",
                       "./PDBx:exptl_crystal_growCategory/PDBx:exptl_crystal_grow/PDBx:pdbx_details"]
            namespace = {'PDBx': 'http://pdbml.pdb.org/schema/pdbx-v40.xsd'}

            # Entry ID
            element = root.find(queries[0], namespace)
            try:
                # Remove the "-noatom" part
                entry_id = element.get('datablockName')[:-7]
                data.append(entry_id)
            except:
                pprint("The entry ID is not provided!")
                continue

            # Number of protein atoms
            element = root.find(queries[1], namespace)
            try:
                protein_count = int(element.text)
            except:
                pprint("The number of protein atoms is not provided!")
                continue

            # Resolution of structure
            element = root.find(queries[2], namespace)
            try:
                resolution = float(element.text)
                data.append(resolution)
            except:
                pprint("A resolution is not provided!")
                continue

            # Method used to analyze structure
            element = root.find(queries[3], namespace)
            try:
                method = element.get('method')
            except:
                pprint("An experimental method is not provided!")
                continue

            # Details of the crystallization conditions for the structure
            element = root.find(queries[4], namespace)
            try:
                details = element.text
                data.append(details)
            except:
                pprint("The details section does not exist!")
                continue

            if not details:
                pprint("The details section is empty!")
                continue

            if details.isdigit():
                pprint("The details section only contains numbers: {0}".format(details))
                continue

            if protein_count > 0 and resolution <= min_resolution and method == exptl_method:
                # Insert each row of values from data
                sql.add_entry_data_row(db_connection, (data,))

                # Commit changes and disconnect from the database
                sql.commit_changes(db_connection)
        except etree.ParseError:
            pprint("There was an error parsing the file located at {0}".format(file))


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
