__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from pprint import pprint

from Packages.pdb_data_mining import download_ftp as ftp
from Packages.pdb_data_mining import parse_xml as xml
from Packages.pdb_data_mining import manage_sql as sql
from Packages.pdb_data_analysis import identify_valid_names as identify
from Packages.pdb_data_analysis import query_entries as query

import os

# CONSTANTS

# For Laptop Tests
L_BASE_DB = "E:/Code/Python Projects/pdb-data-mining/Database/information.db"
L_BASE_XML = "E:/Code/Python Projects/Storage/PDB/XML-no-atom/"

# For FTP connection
BASE_XML_FULL = "/storage2/vlad/PDB/XML/"
BASE_XML_NO_ATOM = "/storage2/vlad/PDB/XML-no-atom/"
XML_FULL = "/pub/pdb/data/structures/divided/XML/"
XML_NO_ATOM = "/pub/pdb/data/structures/divided/XML-noatom/"

# For parsing contents of files
RAW_COLUMNS = 9
RAW_COORDINATE_COLUMNS = 9
SORTED_COLUMNS = 7

# For managing SQL database
BASE_DB = "/home/vlad/Documents/Code/pdb-data-mining/Database/information.db"

# For managing text output
BASE_OUTPUT = "/home/vlad/Documents/Code/pdb-data-mining/Output/"

# For chemical name searches
non_ionic = ["2-ethoxyethanol", "2-hydroxyethyldisulfide", "2-mercaptoethanol", "2-methylpentane-2,4-diol", "acetone",
             "atp", "beta-mercaptoethanol", "beta-octyl glucoside", "bme",
             "butanol", "cymal", "d-camphor", "dioxane",
             "dithiothreitol", "ddt", "dioxane", "dmso", "dte", "dtt", "edta", "ethanol", "ethylene glycol",
             "glucose", "glycerol", "gsh", "hexanediol",
             "jeffamine", "l-cysteine", "l-proline", "mercaptoethanol", "methanol",
             "mme", "mpd", "n-bog", "nad+",
             "nadp", "nan3", "nascn", "peg", "pentaerythritol propoxylate", "phenol", "polyethylene glycol", "propanol",
             "sucrose", "tmao", "trimethylamine n-oxide", "urea", "xylitol"]
ammonium_sulfate_aliases = ["ammonium sulfate", "ammonium sulphate", "amonium sulfate", "ammonia sulfate",
                            "ammomium sulfate", "ammonium-sulfate", "ammonium suflate", "ammonium sulf",
                            "ammonuim sulfate", "ammounium sulfate", "ammonium_sulfate", "amm sulfate",
                            "ammoium sulfate", "ammonium  sulfate", "ammonium salfate", "ammoniun sulfate",
                            "ammonium sufate", "ammonuim sulphate",
                            "ams", "as", "amsulfate", "ammoniumsulfate", "ammoniumsulphate", "amoniumsulfate",
                            "ammomiumsulfate", "ammoniumsuflate", "ammonuimsulphate",
                            "amso4", "am so4", "nh4 sulfate", "nh4sulfate", "ammso4", "amm so4", "am2so4", "am2 so4",
                            "nh4 sulphate", "nh4sulphate", "ammonium so4", "ammoniumso4",
                            "(nh4)2so4", "(nh4)2 so4", "nh4so4", "nh4 so4", "(nh4)so4", "(nh4) so4",
                            "(nh4)2s04", "(nh4)2 s04", "(nh4)2(so4)", "(nh4)2 (so4)"]

# MAIN BODY


# Download structures from the FTP site
def ftp_download(ftp_base, local_filebase):
    ftp_conn = ftp.connect(ftp_base)
    # Commence the downloading process
    ftp.download_all(local_filebase, ftp_conn)
    # Once all of the specified files have been downloaded, disconnect from the site
    ftp.disconnect(ftp_conn)

# Parse through all of the newly downloaded structures
def parse_xml_files(local_filebase, local_database) -> object:
    # Connect to the database
    connection = sql.connect_database(local_database)

    # Clean the existing contents of the table
    # sql.delete_all_rows(connection, "entry_data")
    # Remove the existing table so that the number of columns may be adjusted
    # sql.delete_table(connection, "entry_data")

    # Add a new table to the database
    # sql.create_entry_data_table(connection)

    files = xml.discover_file_names(local_filebase)
    xml.extract_data(files, connection, 2.50, "X-RAY DIFFRACTION")

    # Disconnect from the database
    sql.close_database(connection)


def identify_names(local_database):
    connection = sql.connect_database(local_database)

    # Find all of the chemical names stored in the crystallization information of the structures
    interpretable_entries = identify.discover_interpretable_entries(connection)
    identify.parse_crystallization_chemicals(connection, interpretable_entries)

    sql.close_database(connection)


def print_chemical_names(local_database, output_base):
    connection = sql.connect_database(local_database)
    cursor = connection.cursor()

    command = "SELECT COUNT(*), name FROM crystallization_chemicals GROUP BY name " \
              "HAVING COUNT(*) > 2 ORDER BY COUNT(*) DESC"

    filename = os.path.join(output_base, "most-common-chemical-names.txt")
    file = open(filename, 'w')
    file.write("CHEMICAL NAMES, DISPLAYED BY THEIR FREQUENCY IN PDB ENTRIES \n\n")
    for index, row in enumerate(cursor.execute(command)):
        count = row[0]
        chemical_name = row[1]
        file.write("{0}. \n".format(index + 1))
        file.write("'{0}' occurred '{1}' times in all of the entries extracted from the PDB. \n\n"
                   .format(chemical_name, count))

        # result_command = "SELECT parent_id, name, concentration, unit FROM crystallization_chemicals " \
        #                  "GROUP BY parent_id HAVING COUNT(*) > 2"
        #
        # new_filename = os.path.join(output_base, "results.txt")
        # file = open(new_filename, 'w')
        # file.write("RESULTS FOR DISPLAY IN THE FINAL PAPER \n\n")
        # for index, row in enumerate(cursor.execute(result_command)):
        #     id = row[0]
        #     chemical_name = row[1]
        #     value = row[2]
        #     unit = row[3]
        #     file.write("{0} | {1} [{2} {3}] \n\n".format(id, chemical_name, value, unit))
        # sql.close_database(connection)


def store_reference_list(local_database, data):
    connection = sql.connect_database(local_database)

    sql.delete_all_rows(connection, "crystallization_chemicals")
    sql.create_crystallization_chemicals_table(connection)
    sql.add_crystallization_chemicals_row(connection, data)
    sql.commit_changes(connection)

    sql.close_database(connection)

# ftp_download(XML_NO_ATOM, BASE_XML_NO_ATOM)
# parse_xml_files(BASE_XML_NO_ATOM, BASE_DB)
# identify_names(BASE_DB)

# connection = sql.connect_database(BASE_DB)
# query.standardize_names(connection)
#
# sql.commit_changes(connection)
# sql.close_database(connection)

# print_chemical_names(BASE_DB, BASE_OUTPUT)
