
__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from Packages.pdb_data_mining import download_ftp as FTP
from Packages.pdb_data_mining import parse_text as TEXT
from Packages.pdb_data_mining import manage_sql as SQL

from pprint import pprint

# CONSTANTS

# For FTP connection
BASE_TEXT = "/storage2/vlad/PDB/text/"
BASE_XML_FULL = "/storage2/vlad/PDB/XML/"
BASE_XML_NO_ATOM = "/storage2/vlad/PDB/XML-no-atom/"
XML_FULL = "/pub/pdb/data/structures/divided/XML/"
XML_NO_ATOM = "/pub/pdb/data/structures/divided/XML-noatom/"
TEXT = "/pub/pdb/data/structures/divided/pdb/"

# For parsing contents of files
COLUMNS = 4
DATA_COLUMNS = 3

# For managing SQL database
BASE_DB = "/home/Documents/Code/pdb-data-ming/Database/information.db"

# MAIN BODY

ftp_conn = FTP.connect(XML_NO_ATOM)
archive_count = FTP.count_archives(BASE_XML_NO_ATOM, ftp_conn)
pprint("There are a total of {0} directories in the FTP site!".format(archive_count))
pprint("To divide this into 4 separate processes: {0}".format(archive_count / 4))
start = input("Where would you like to start downloading: ")
start_point = int(start)
end = input("Where would you like to end downloading: ")
end_point = int(end)
FTP.download(BASE_XML_NO_ATOM, ftp_conn, start_point, end_point)
FTP.disconnect(BASE_XML_NO_ATOM, ftp_conn)



# Create a new database
connection = SQL.connect_database(BASE_DB)
# Add a new table to the database
SQL.add_table(connection, "information")
# Insert a row of values
# add_row(connection, "information", listing)
#
# print_database(connection, "information", "resolution")
#
# Commit changes and disconnect from the datbase
SQL.commit_changes(connection)
