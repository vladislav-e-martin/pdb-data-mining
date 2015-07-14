PDB Data Mining

Author: Vladislav Martin
Workplace: Frick Chemistry Laboratory, Princeton University
Reason: Molecular Biophysics REU Program
Beneficiary: Jannette Carey, Principal Investigator

CONTENTS

* pdb_data_mining

1. download_ftp.py - Download and decompress protein structures from the wwPDB FTP site

2. parse_xml.py - Extract relevant data from acceptable protein structures

	+ Criteria defining what is "acceptable" can be found in the source code

3. database_sql.py - Store relevant data from acceptable protein structures into a database

* pdb_data_analysis

1. create_golden_list.py - Analyze the contents of the "entry_data" table in "information.db" and parse each entry's details section
for the names of chemicals used to crystallize the proteins

NOTE:
The results of create_golden_list are stored in the "crystallization_chemicals" and "golden_chemical_reference" tables
    + "crystallization_chemicals" table stores ALL chemical names discovered
    + "golden_chemical_reference" table stores ONLY chemical names that occurred 10 or more times within all of the
    entries' details sections

2. query_entries.py - Query the "golden_chemical_reference" table for those entries that only contain chemicals contained
in a manually generated list of chemical names