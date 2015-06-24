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
L_XML_NO_ATOM = "/pub/pdb/data/structures/divided/XML-noatom/"

# For FTP connection
BASE_TEXT = "/storage2/vlad/PDB/text/"
BASE_XML_FULL = "/storage2/vlad/PDB/XML/"
BASE_XML_NO_ATOM = "/storage2/vlad/PDB/XML-no-atom/"
XML_FULL = "/pub/pdb/data/structures/divided/XML/"
XML_NO_ATOM = "/pub/pdb/data/structures/divided/XML-noatom/"
TEXT = "/pub/pdb/data/structures/divided/pdb/"

# For parsing contents of files
COLUMNS = 9
DB_COLUMNS = 7

# For managing SQL database
BASE_DB = "/home/Documents/Code/pdb-data-ming/Database/information.db"

# MAIN BODY

# Connect to the database
connection = ANALYZE.connect_database(L_BASE_DB)

# Find matches in the database
matches = ANALYZE.find_common_chemicals(connection, "information", "id")
pprint(matches)
pprint("Matches found: {0}, {1}, {2}".format(len(matches[0]), len(matches[1]), len(matches[2])))

# ftp_conn = FTP.connect(L_XML_NO_ATOM)
# # archive_count = FTP.count_archives(L_BASE_XML, ftp_conn)
# # pprint("There are a total of {0} directories in the FTP site!".format(archive_count))
# # pprint("To divide this into 4 separate processes: {0}".format(archive_count / 4))
# # start = input("Where would you like to start downloading: ")
# # start_point = int(start)
# # end = input("Where would you like to end downloading: ")
# # end_point = int(end)
# FTP.download(L_BASE_XML, ftp_conn, 798, 800)
# FTP.disconnect(ftp_conn)
#
# files = XML.fill_list(L_BASE_XML)
# raw_data = XML.fill_raw(files, COLUMNS)
# sorted_indices = XML.filter_indices(raw_data)
# sorted_data = XML.fill_sorted(raw_data, sorted_indices, DB_COLUMNS)
#
# # Create a new database
# connection = SQL.connect_database(L_BASE_DB)
# # Add a new table to the database
# SQL.add_table(connection, "information")
# # Insert each row of values from sorted_data
# SQL.add_row(connection, "information", sorted_data)
# # Print the contents of the database as confirmation
# SQL.print_database(connection, "information", "id")
# # Commit changes and disconnect from the database
# SQL.commit_changes(connection)
