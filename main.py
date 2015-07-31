__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from pprint import pprint

from Packages.pdb_data_mining import download_ftp as ftp
from Packages.pdb_data_mining import parse_xml as xml
from Packages.pdb_data_mining import manage_sql as sql
from Packages.pdb_data_analysis import identify_valid_names as identify
from Packages.pdb_data_analysis import query_entries as query
from Packages.pdb_data_analysis import export_data as export


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
ethylene_glycol_aliases = ["ethylene glycol", "ethyleneglycol", "etgly", "et gly", "eg", "e g"]


# MAIN BODY


# Download structures from the FTP site
def ftp_download(ftp_base, local_filebase):
    ftp_conn = ftp.connect(ftp_base)
    # Commence the downloading process
    ftp.download_all(local_filebase, ftp_conn)
    folder_count = ftp.count_directories(ftp_conn)
    # Once all of the files have been downloaded from the specified section of the data bank, disconnect from the site
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



def store_reference_list(local_database, data):
    connection = sql.connect_database(local_database)

    sql.delete_all_rows(connection, "crystallization_chemicals")
    sql.create_crystallization_chemicals_table(connection)
    sql.add_crystallization_chemicals_row(connection, data)
    sql.commit_changes(connection)

    sql.close_database(connection)

# ftp_download(XML_NO_ATOM, BASE_XML_NO_ATOM)
# parse_xml_files(BASE_XML_NO_ATOM, BASE_DB)
identify_names(BASE_DB)
