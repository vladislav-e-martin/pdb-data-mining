
__author__ = 'Vladislav Martin'

# LIBRARIES AND PACKAGES

from Packages.pdb_data_mining import download_ftp as FTP
from Packages.pdb_data_mining import parse_xml as XML
from Packages.pdb_data_mining import database_sql as SQL
from Packages.pdb_data_mining import analyze_string as STRING

from pprint import pprint

# CONSTANTS

BASE = "/storage2/vlad/Test/PDB"
COLUMNS = 4
DATA_COLUMNS = 3

# MAIN BODY

with open("/storage2/vlad/Test/all_keywords.txt", 'r') as fileinput:
   for line in fileinput:
       line = line.lower()

#listing = XML.fill_list(BASE)

#raw_data = XML.fill_raw(listing, COLUMNS)

#sorted_rows_list = XML.filter_indices(raw_data, COLUMNS)
#pprint("There are {0} structures that fit the criteria we have provided!".format(len(sorted_rows_list)))
#sorted = XML.fill_sorted(listing, sorted_rows_list, DATA_COLUMNS)

#details_list = XML.fill_details(listing, sorted_rows_list)

#STRING.identify_keywords(details_list)

# # Create a new database
# connection = connect_database(BASE)
# # Add a new table to the database
# add_table(connection, "information")
# # Insert a row of values
# add_row(connection, "information", listing)
#
# print_database(connection, "information", "resolution")
#
# # Commit changes and disconnect from the datbase
# commit_changes(connection)