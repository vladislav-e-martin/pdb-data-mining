# LIBRARIES AND PACKAGES

import xml.etree.ElementTree as ET
import os
from pprint import pprint
import sys

# CONSTANTS

BASE = "E:/Code/Python Projects/pdb-data-mining/PDB/"
COLUMNS = 12

# FUNCTION DECLARATIONS

# Fill a list with the filepaths to all downloaded structures
def fill(base):
    xml_files = []
    for root, directories, files in os.walk(base):
        pprint ("Root: {0} Directories: {1} Files: {2}".format(root, len(directories), len(files)))
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

# Extract only the data we are interested in into a list
def extract(files, columns):
    rows = len(files)
    data = [[0 for x in range(columns)] for x in range(rows)]
    
    for index, file in enumerate(files):
        # Check if the file can be parsed
        try:
            pprint("Current index: {0}".format(index))
            tree = ET.parse(file)

            root = tree.getroot()
            # Make XPATHs simpler
            namespace = {'PDBx': 'http://pdbml.pdb.org/schema/pdbx-v40.xsd'}

            current_col = 0

            for element in root.findall("."):
                pprint(element)
                id = element.get('datablockName')
                data[index][current_col] = id
                pprint(data[index][current_col])
                current_col += 1

            # Number of protein atoms
            for element in root.findall("./PDBx:refine_histCategory/PDBx:refine_hist/PDBx:pdbx_number_atoms_protein", namespace):
                count = int(element.text)
                data[index][current_col] = count
                pprint(data[index][current_col])
                current_col += 1

            # Resolution of structure
            for element in root.findall("./PDBx:reflnsCategory/PDBx:reflns/PDBx:d_resolution_high", namespace):
                resolution = float(element.text)
                data[index][current_col] = resolution
                pprint(data[index][current_col])
                current_col += 1

            # Method used to analyze structure
            for element in root.findall("./PDBx:exptlCategory/PDBx:exptl", namespace):
                method = element.get('method')
                data[index][current_col] = method
                pprint(data[index][current_col])
                current_col += 1

            # Matthew's density and density percent for solution
            for tag in root.findall("./PDBx:exptl_crystalCategory/PDBx:exptl_crystal", namespace):
                for element in tag:
                    data[index][current_col] = element.text
                    pprint(data[index][current_col])
                    current_col += 1

            element_count = 0
            max_count = 0
            # Methods for crystallization, pH of entire setup, temperature of entire setup, and details (everything from pH to end can be ignored in details section)
            for tag in root.findall("./PDBx:exptl_crystal_growCategory/PDBx:exptl_crystal_grow", namespace):
                for element in tag:
                    data[index][current_col] = element.text
                    pprint(data[index][current_col])
                    current_col += 1
                    element_count += 1
                    if element_count > max_count:
                        max_count = element_count
        # If it cannot be parsed, it is corrupted and must be deleted and re-downloaded
        except:
            pprint("Unexpected error:", sys.exc_info()[0])
            raise
            #pprint("This XML file is corrupt (either a result of improper communication with the FTP site or an interrupted downloading process).")
            #pprint("Deleting the corrupt XML file.")
            #os.remove(file)
    pprint("The max count for grow conditions: {0}".format(max_count))
    return data

# Sort the structures that do not meet the criteria from the list
def sort(unsorted, columns):
    # Number of rows contained in the refined list
    r_row = 0

    refined = [[0 for x in range(columns)] for x in range(100)]

    # CRITERIA
    # Atom index
    a_i = 1
    # Resolution index
    r_i = 2
    resolution = 2.50
    # Method index
    m_i = 3
    method = "X-RAY DIFFRACTION"

    # Check the rows against the specified criteria
    for index in range(len(unsorted)):
        pprint("Index: {0} out of Total: {1}".format(index, len(unsorted)))
        # Store the rows if they meet the criteria
        try:
            # len(unsorted[index]) > 5
            if unsorted[index][a_i] > 0 and unsorted[index][r_i] <= resolution and unsorted[index][m_i] == method:
                refined[r_row] = unsorted[index]
                pprint("This structure meets the criteria!")
                pprint("Atom count: {0}  Resolution: {1}  Method: {2}  Length: {3}".format(refined[r_row][a_i], refined[r_row][r_i], refined[r_row][m_i], len(refined[r_row])))
                r_row += 1
            # Ignore the rows if they do not meet the criteria
            else:
                pprint("This structure does not meet the criteria.")
        except:
            pprint("Unexpected error:", sys.exc_info()[0])
            raise
            pprint("One of the criteria was not provided with the structure. This structure will not be included in the data set.")
    
    return refined    
# VARIABLE DECLARATIONS

rows = 100

# 2D list to store only relevant information of each downloaded structure
raw_data = [[0 for x in range(COLUMNS)] for x in range(rows)]
# 2D list to store only relevant information of acceptable structures
sorted_data = [[0 for x in range(COLUMNS)] for x in range(rows)]

# MAIN BODY

listing = fill(BASE)
raw_data = extract(listing, COLUMNS)
sorted_data = sort(raw_data, COLUMNS)

pprint("{0} structures fit the criteria provided.".format(len(sorted_data)))
