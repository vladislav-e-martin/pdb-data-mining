__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from Packages.pdb_data_mining import download_ftp as FTP
from Packages.pdb_data_mining import parse_xml as XML
from Packages.pdb_data_mining import manage_sql as SQL
from Packages.pdb_data_analysis import analyze_details as ANALYZE

from pprint import pprint

# CONSTANTS

# For Laptop Tests
L_BASE_DB = "E:/Code/Python Projects/pdb-data-mining/Database/information.db"
L_BASE_XML = "E:/Code/Python Projects/Storage/PDB/XML-no-atom/"

# For FTP connection
BASE_TEXT = "/storage2/vlad/PDB/text/"
BASE_XML_FULL = "/storage2/vlad/PDB/XML/"
BASE_XML_NO_ATOM = "/storage2/vlad/PDB/XML-no-atom/"
XML_FULL = "/pub/pdb/data/structures/divided/XML/"
XML_NO_ATOM = "/pub/pdb/data/structures/divided/XML-noatom/"
TEXT = "/pub/pdb/data/structures/divided/pdb/"

# For parsing contents of files
RAW_COLUMNS = 9
SORTED_COLUMNS = 7

# For managing SQL database
BASE_DB = "/home/vlad/Documents/Code/pdb-data-mining/Database/information.db"

# For chemical name searches
non_ionic = ["peg", "dtt", "edta", "tcep", "bme", "dithiothreitol", "nan3", "atp", "beta-mercaptoethanol", "kscn",
             "2-mercaptoethanol", "gsh", "ada", "adp", "ddt", "nad+", "l-cysteine", "nascn", "mib", "l-proline", "nadp"]

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


# Store the important structures in the database for further analysis
def store_in_information_table(local_base, sorted_data):
	connection = SQL.connect_database(local_base)
	# Add a new table to the database
	SQL.add_table(connection, "information", True, False)
	# Insert each row of values from sorted_data
	SQL.add_parent_row(connection, "information", sorted_data)
	# Print the contents of the database as confirmation
	SQL.print_database(connection, "information", "id")
	# Commit changes and disconnect from the database
	SQL.commit_changes(connection)


def store_in_chemicals_table(local_base):
	connection = SQL.connect_database(local_base)
	# Find all of the chemicals stored in the crystallization information of the structures
	matches = ANALYZE.chemical_name_golden_reference_list(connection, "information", "id")
	# Split the chemicals into their concentrations, the units used to measure concentration,
	# and the name of the chemical itself
	split_matches = ANALYZE.split_results(matches[0], matches[1])
	# Store the results of the split_matches() function into a child table in the database
	SQL.delete_all_rows(connection, "crystallization_chemicals")
	ANALYZE.store_results(connection, "crystallization_chemicals", split_matches)
	# Display the frequency of each chemical name stored in the child table
	ANALYZE.create_common_chemical_names_list(connection, "crystallization_chemicals", "chemical_name")


store_in_chemicals_table(BASE_DB)
