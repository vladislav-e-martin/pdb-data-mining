# LIBRARIES AND PACKAGES

import xml.etree.ElementTree as ET
import os
from pprint import pprint

# CONSTANTS

BASE = "/home/vlad/Documents/Code/pdb-data-mining/PDB/"

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
def extract(files):
    data = []
    data.append([])
    # data = [[0 for x in range(columns)] for x in range(rows)]
    
    for index, file in enumerate(files):
        # Check if the file can be parsed
        try:
            tree = ET.parse(file)
        # If it cannot be parsed, it is corrupted and must be deleted and re-downloaded
        except:
            pprint("This XML file is corrupt (either a result of improper communication with the FTP site or an interrupted downloading process).")
            pprint("Deleting the corrupt XML file.")
            os.remove(file)

    root = tree.getroot()
    # Make XPATHs simpler
    namespace = {'PDBx': 'http://pdbml.pdb.org/schema/pdbx-v40.xsd'}

    # Number of protein atoms
    for element in root.findall("./PDBx:refine_histCategory/PDBx:refine_hist/PDBx:pdbx_number_atoms_protein", namespace):
        count = int(element.text)
        data[index].append(count)
    
    # Resolution of structure
    for element in root.findall("./PDBx:reflnsCategory/PDBx:reflns/PDBx:d_resolution_high", namespace):
        resolution = float(element.text)
        data[index].append(resolution)

    # Method used to analyze structure
    for element in root.findall("./PDBx:exptlCategory/PDBx:exptl", namespace):
        method = element.get('method')
        data[index].append(method)
    
    # Matthew's density and density percent for solution
    for element in root.findall("./PDBx:exptl_crystalCategory/PDBx:exptl_crystal", namespace):
        pprint("Contents: " + element.text)
    # Methods for crystallization, pH of entire setup, temperature of entire setup, and details (everything from pH to end can be ignored in details section)
    for element in root.findall("./PDBx:exptl_crystal_growCategory/PDBx:exptl_crystal_grow", namespace):
        pprint("Contents: " + element.text)

    return data

# Sort the structures that do not meet the criteria from the list
def sort(unsorted):
    # Number of rows contained in the refined list
    r_row = 0
    refined = []

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
    for row in unsorted:
        # Store the rows if they meet the criteria
        if row[a_i] > 0 and row[r_i] >= resolution and row[m_i] == method and len(row) > 5:
            refined.append(row)
            pprint("This structure meets the criteria!")
            pprint("Atom count: {0}  Resolution: {1}  Method: {2}  Length: {3}".format(refined[r_row][a_i], refined[r_row][r_i], refined[r_row][m_i], len(refined[r_row])))
            input("Press Enter to continue...")
            r_row += 1
        # Ignore the rows if they do not meet the criteria
        else:
            pprint("This structure does not meet the criteria")
            pprint("Atom count: {0}  Resolution: {1}  Method: {2}  Length: {3}".format(refined[r_row][a_i], refined[r_row][r_i], refined[r_row][m_i], len(refined[r_row])))
            input("Press Enter to continue...")
    
    return refined    
# VARIABLE DECLARATIONS

# 2D list to store only relevant information of each downloaded structure
data = [[]]
# 2D list to store only relevant information of acceptable structures
sorted_data = [[]]

# MAIN BODY

listing = fill(BASE)
data = extract(listing)
sorted_data = sort(data)
