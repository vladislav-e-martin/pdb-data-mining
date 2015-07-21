__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from pprint import pprint

from Packages.pdb_data_mining import download_ftp as ftp
from Packages.pdb_data_mining import parse_xml as xml
from Packages.pdb_data_mining import manage_sql as sql
from Packages.pdb_data_analysis import create_golden_list as create
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
    archive_count = ftp.count_archives(ftp_conn)
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
    ftp.download(local_filebase, ftp_conn, start_point, end_point)
    # Once all of the files have been downloaded from the specified section of the data bank, disconnect from the site
    ftp.disconnect(ftp_conn)


def ftp_download_full(ftp_base, local_filebase, ids):
    ftp_conn = ftp.connect(ftp_base)
    ftp.download_specific_files(local_filebase, ftp_conn, ids)
    ftp.disconnect(ftp_conn)

# Parse through all of the newly downloaded structures
def parse_raw_data(local_filebase, raw_columns, sorted_columns) -> object:
    files = xml.list_files(local_filebase, len(local_filebase))
    raw_data = xml.fill_raw(files, raw_columns)
    sorted_indices = xml.filter_indices(raw_data)
    sorted_data = xml.fill_sorted(raw_data, sorted_indices, sorted_columns)

    return sorted_data


# REMEMBER TO DELETE THE "information" TABLE (NOT DONE YET)
# Store the important structures in the database for further analysis
def store_in_entry_table(local_database, data):
    connection = sql.connect_database(local_database)

    # Add a new table to the database
    sql.create_entry_data_table(connection)

    # Insert each row of values from sorted_data
    sql.add_entry_data_row(connection, data)

    # Commit changes and disconnect from the database
    sql.commit_changes(connection)

    sql.close_database(connection)


def create_reference_list(local_database):
    connection = sql.connect_database(local_database)

    # Find all of the chemical names stored in the crystallization information of the structures
    delimited = create.discover_common_delimited_entries(connection)
    raw_list = create.create_golden_reference_list(connection, delimited)
    refined_list = create.refine_golden_reference_list(raw_list)
    final_list = create.finalize_golden_reference_list(refined_list)

    return final_list


def store_reference_list(local_database, data):
    connection = sql.connect_database(local_database)

    sql.delete_all_rows(connection, "crystallization_chemicals")
    sql.create_crystallization_chemicals_table(connection)
    sql.add_crystallization_chemicals_row(connection, data)
    sql.commit_changes(connection)

    sql.close_database(connection)


def search_table_for_chemicals(local_database):
    connection = sql.connect_database(local_database)

    # sql.delete_all_rows(connection, "aliases")
    # create.add_to_aliases(connection, "ammonium sulfate", ammonium_sulfate_aliases)
    # create.add_to_aliases(connection, "ethylene glycol", ethylene_glycol_aliases)
    #
    # query.standardize_chemical_names(connection)
    #
    # query.query_for_match(connection, "ammonium sulfate")
    #
    # sql.commit_changes(connection)
    #
    # sql.print_table(connection, "crystallization_chemicals", "parent_id")

    ammonium_sulfate_matches = query.search_for_chemical(connection, ammonium_sulfate_aliases, 1)
    ammonium_sulfate_matches.remove("1T4Q")
    non_ionic_matches = query.search_for_chemical(connection, non_ionic, 50)

    # pprint("Length of ammonium sulfate matches: {0}".format(len(ammonium_sulfate_matches)))
    # pprint("Length of non-ionic matches: {0}".format(len(non_ionic_matches)))

    export.export_concentration_data(connection, ammonium_sulfate_matches, "ammonium-sulfate-concentrations")
    export.export_coordinate_data(connection, ammonium_sulfate_matches, "ionic-atom-coordinates")
    export.export_coordinate_data(connection, non_ionic_matches, "non-ionic-atom-coordinates")

    # for index, match in enumerate(ammonium_sulfate_matches):
    #     ammonium_sulfate_matches[index] = match.lower()
    #
    # for index, match in enumerate(non_ionic_matches):
    #     non_ionic_matches[index] = match.lower()

    # ftp_download_full(XML_FULL, BASE_XML_FULL, ammonium_sulfate_matches)
    # ftp_download_full(XML_FULL, BASE_XML_FULL, non_ionic_matches)

    # ammonium_sulfate_files = xml.list_files(BASE_XML_FULL, ammonium_sulfate_matches)
    # non_ionic_files = xml.list_files(BASE_XML_FULL, non_ionic_matches)
    #
    # ammonium_sulfate_raw_data = xml.fill_raw_coordinates(ammonium_sulfate_files, RAW_COORDINATE_COLUMNS, 1)
    # non_ionic_raw_data = xml.fill_raw_coordinates(non_ionic_files, RAW_COORDINATE_COLUMNS, 0)
    #
    # sql.delete_all_rows(connection, "entry_coordinate_data")
    # sql.delete_table(connection, "entry_coordinate_data")
    # sql.create_entry_coordinate_data_table(connection)
    # sql.add_entry_coordinate_data_row(connection, ammonium_sulfate_raw_data)
    # sql.add_entry_coordinate_data_row(connection, non_ionic_raw_data)
    # sql.commit_changes(connection)
    #
    # sql.close_database(connection)

# data_to_store = parse_raw_data(BASE_XML_NO_ATOM, RAW_COLUMNS, SORTED_COLUMNS)

# store_in_entry_table(BASE_DB, data_to_store)

# reference_list = create_reference_list(BASE_DB)
# pprint(len(reference_list))
# store_reference_list(BASE_DB, reference_list)

search_table_for_chemicals(BASE_DB)

connection = sql.connect_database(BASE_DB)
sql.print_table(connection, "entry_coordinate_data", "parent_id")
