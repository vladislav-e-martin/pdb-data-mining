__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from pprint import pprint

from Packages.pdb_data_mining import download_ftp as FTP
from Packages.pdb_data_mining import parse_xml as XML
from Packages.pdb_data_mining import manage_sql as SQL
from Packages.pdb_data_analysis import create_golden_list as CREATE


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
SORTED_COLUMNS = 7

# For managing SQL database
BASE_DB = "/home/vlad/Documents/Code/pdb-data-mining/Database/information.db"

# For chemical name searches
non_ionic = ["2-ethoxyethanol", "2-hydroxyethyldisulfide", "2-mercaptoethanol", "2-methylpentane-2,4-diol", "acetone",
             "atp", "beta-mercaptoethanol", "beta-octyl glucoside", "bme",
             "butanol", "cymal", "d-camphor", "dioxane",
             "dithiothreitol", "ddt", "dioxane", "dmso", "dte", "dtt", "edta", "ethanol", "etgly", "ethylene glycol",
             "glucose", "glycerol", "gsh", "hexanediol",
             "jeffamine", "l-cysteine", "l-proline", "mercaptoethanol", "methanol",
             "mme", "mpd", "n-bog", "nad+",
             "nadp", "nan3", "nascn", "peg", "pentaerythritol propoxylate", "phenol", "polyethylene glycol", "propanol",
             "sucrose", "tmao", "trimethylamine n-oxide", "urea", "xylitol"]
ammonium_sulfate = ["ammonium sulfate", "ammonium sulphate", "(nh4)2so4", "amso4", "nh4so4", "ammoniumsulfate", "ams",
                    "(nh4)so4", "as", "amonium sulfate", "nh4 sulfate", "ammomium sulfate", "ammonium-sulfate",
                    "am2so4", "ammounium sulfate", "(nh4)2 so4", "ammonium_sulfate", "ammso4", "(nh4)2s04",
                    "ammonium suflate", "ammonium sulf", "ammonuim sulfate", "amsulfate", "nh4 sulphate", "(nh4)2(so4)",
                    "amm sulfate", "ammoium sulfate", "ammonium  sulfate", "ammonium salfate", "ammonium so4",
                    "ammoniumsulphate", "ammoniun sulfate", "ammonia sulfate", "ammonium sufate"]
# FIX THE LAST ONE IN A FILTER AT SOME POINT

# MAIN BODY

# Download structures from the FTP site
def ftp_download(ftp_base, local_base):
    ftp_conn = FTP.connect(ftp_base)
    archive_count = FTP.count_archives(ftp_conn)
    # Provide the user with information to split the downloading workload between several processes
    pprint("There are a total of {0} directories in the FTP site!".format(archive_count))
    # Specifically, 4 processes
    pprint("To divide this into 4 separate processes: {0}".format(archive_count / 4))
    # Ask the user at which directory the downloading process should begin
    start = input("Where would you like to start downloading: ")
    start_point = int(start)
    # Ask the user at which directory the downloading process should end
    end = input("Where would you like to end downloading: ")
    end_point = int(end)
    # Commence the downloading process
    FTP.download(local_base, ftp_conn, start_point, end_point)
    # Once all of the files have been downloaded from the specified section of the data bank, disconnect from the site
    FTP.disconnect(ftp_conn)


# Parse through all of the newly downloaded structures
def parse_raw_data(local_base, raw_columns, sorted_columns) -> object:
    files = XML.fill_list(local_base)
    raw_data = XML.fill_raw(files, raw_columns)
    sorted_indices = XML.filter_indices(raw_data)
    sorted_data = XML.fill_sorted(raw_data, sorted_indices, sorted_columns)

    return sorted_data


# REMEMBER TO DELETE THE "information" TABLE (NOT DONE YET)
# Store the important structures in the database for further analysis
def store_in_entry_table(local_base, sorted_data):
    connection = SQL.connect_database(local_base)
    # Add a new table to the database
    SQL.add_table(connection, "entry_data", True, False)
    # Insert each row of values from sorted_data
    SQL.add_parent_row(connection, "entry_data", sorted_data)
    # Print the contents of the database as confirmation
    # SQL.print_database(connection, "entry_data", "id")
    # Commit changes and disconnect from the database
    SQL.commit_changes(connection)


def create_reference_list(local_base):
    connection = SQL.connect_database(local_base)

    # Find all of the chemical names stored in the crystallization information of the structures
    delimited = CREATE.discover_common_delimited_entries(connection)
    raw_list = CREATE.create_golden_reference_list(connection, delimited)
    refined_list = CREATE.refine_golden_reference_list(raw_list)
    final_list = CREATE.finalize_golden_reference_list(refined_list)

    return final_list


def store_reference_list(local_base, data):
    connection = SQL.connect_database(local_base)

    # Store the final reference list into the crystallization_chemicals table
    SQL.delete_all_rows(connection, "crystallization_chemicals")
    SQL.delete_table(connection, "information")
    SQL.create_crystallization_chemicals_table(connection)
    SQL.add_crystallization_chemicals_row(connection, data)
    SQL.commit_changes(connection)

    test = "SELECT id, details FROM information, crystallization_chemicals WHERE " \
           "crystallization_chemicals.chemical_name = 'ammonium sulfate' AND " \
           "entry_data.id = crystallization_chemicals.parent_entry_id"

    cursor = connection.cursor()

    results = cursor.execute(test).fetchall()

    for result in results:
        entry_id = result[1]
        name = result[4]
        pprint("Entry ID: {0}       Chemical Name: {1}".format(entry_id, name))
    pprint("There were a total of {0} matches.".format(len(results)))


data_to_store = parse_raw_data(BASE_XML_NO_ATOM, RAW_COLUMNS, SORTED_COLUMNS)
store_in_entry_table(BASE_DB)

reference_list = create_reference_list(BASE_DB)
store_reference_list(BASE_DB, reference_list)
