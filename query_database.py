from pprint import pprint

from Packages.pdb_data_mining import manage_sql as SQL

BASE_DB = "/home/vlad/Documents/Code/pdb-data-mining/Database/information.db"

connection = SQL.connect_database(BASE_DB)

cursor = connection.cursor()

golden_command = "SELECT DISTINCT chemical_name FROM crystallization_chemicals"

parent_command = "SELECT id, details FROM information, crystallization_chemicals WHERE " \
                 "crystallization_chemicals.chemical_name = 'ammonium sulfate' AND " \
                 "entry_data.id = crystallization_chemicals.parent_entry_id"

golden_result = cursor.execute(golden_command).fetchall()

parent_result = cursor.execute(parent_command).fetchall()

for row in parent_result:
    pprint(row)
