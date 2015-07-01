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
             "2-mercaptoethanol", "gsh", "ada", "adp", "ddt", "nad+"]
ammonium_sulfate = ["ammonium sulfate", "ammonium_sulphate", "amso4", "nh4so4"]
hepes = ["hepes", "hepes-naoh", "hepes/naoh", "hepes buffer", "hepes-na", "na-hepes", "na hepes", "sodium hepes",
         "hepes sodium", "hepes 7"]
tris = ["tris", "tris-hcl", "bis-tris", "bis-tris propane", "tris hcl", "tris/hcl", "tris-cl", "bistris", "trishcl",
        "tris buffer", "tris hydrochloride", "tris-hcl buffer", "bis-tris-propane", "bis-tris buffer",
        "bis tris propane", "bis tris"]
mes = ["mes", "mes buffer", "mes-naoh", "mes monohydrate"]
sodium_chloride = ["nacl", "sodium chloride"]
sodium_acetate = ["sodium acetate", "na acetate", "sodium acetate trihydrate", "na-acetate", "naoac", "naacetate",
                  "sodium acetate buffer", "naac"]
magnesium_chloride = ["mgcl2", "magnesium chloride", "magnesium chloride hexahydrate", "mgcl"]
sodium_citrate = ["sodium citrate", "na citrate", "tri-sodium citrate", "na-citrate", "nacitrate", "tri-sodium citrate",
                  "trisodium citrate", "sodium citrate tribasic dihydrate", "sodium citrate buffer"]
ammonium_acetate = ["ammonium acetate", "nh4ac", "nh4 acetate"]
sodium_cacodylate = ["sodium cacodylate", "na cacodylate", "sodium cacodylate trihydrate", "na-cacodylate",
                     "nacacodylate"]
lithium_sulfate = ["lithium sulfate", "li2so4", "lithium sulphate", "lithium sulfate monohydrate"]
calcium_chloride = ["cacl2", "calcium chloride", "calcium chloride dihydrate"]
imidazole = ["imidazole"]
sodium_formate = ["sodium formate", "na formate"]
calcium_acetate = ["calcium acetate", "ca acetate"]
potassium_phosphate = ["potassium phosphate"]
potassium_chloride = ["kcl", "potassium chloride"]
sodium_phosphate = ["sodium phosphate", "sodium dihydrogen phosphate", "sodium phosphate buffer"]
citrate = ["citrate", "citrate buffer"]
cacodylate = ["cacodylate", "cacodylate buffer"]
sodium_malonate = ["sodium malonate"]
magnesium_acetate = ["magnesium acetate", "mg acetate"]
bicine = ["bicine"]
citric_acid = ["citric acid"]
lithium_chloride = ["licl", "lithium chloride"]
zinc_acetate = ["zinc acetate"]
liso4 = ["liso4"]
acetate = ["acetate", "acetate buffer"]
ches = ["ches"]
sodium_potassium_phosphate = ["na/k phosphate", "sodium/potassium phosphate", "sodium-potassium phosphate"]
ammonium_citrate = ["ammonium citrate"]
manganese_chloride = ["mncl2", "manganese chloride"]
magnesium_sulfate = ["mgso4", "magnesium sulfate"]
ammonium_phosphate = ["ammonium phosphate", "ammonium dihydrogen phosphate", "ammonium phosphate monobasic"]
phosphate = ["phosphate buffer", "phosphate"]
phosphate_citrate = ["phosphate-citrate", "phosphate citrate"]
mops = ["mops"]
k2hpo4 = ["k2hpo4"]
glycine = ["glycine"]
magnesium_formate = ["magnesium formate"]
pipes = ["pipes"]
zinc_chloride = ["zncl2", "zinc chloride"]
kh2po4 = ["kh2po4"]
ammonium_chloride = ["ammonium chloride", "nh4cl"]
zinc_sulfate = ["znso4", "zinc sulfate"]
nah2po4 = ["nah2po4"]
ammonium_formate = ["ammonium formate"]
potassium_thiocyanate = ["potassium thiocyanate"]
succinic_acid = ["succinic327 acid"]
dl_malic_acid = ["dl-malic acid"]
potassium_sodium_tartrate = ["potassium sodium tartrate", "k/na tartrate", "na/k tartrate",
                             "potassium sodium tartrate tetrahydrate", "sodium potassium tartrate"]
btp = ["btp"]
nickel_chloride = ["nicl2"]
magnesium = ["mg"]
sodium_nitrate = ["sodium nitrate", "nano3"]
cdcl2 = ["cdcl2"]
sodium_fluoride = ["naf", "sodium fluoride"]
sodium_iodide = ["nai", "sodium iodide"]
caps = ["caps"]
ammonium_nitrate = ["ammonium nitrate"]
sodium_sulfate = ["sodium sulfate", "na2so4"]
sodium_azide = ["sodium azide"]
potassium_acetate = ["potassium acetate"]
spermine = ["spermine"]
sodium = ["na"]
nh4oac = ["nh4oac"]
sodium_dithionite = ["sodium dithionite"]
cadmium_chloride = ["cadmium chloride"]
sodium_thiocyanate = ["sodium thiocyanate"]
sodium_bromide = ["sodium bromide", "nabr"]
cacodylic_acid = ["cacodylic acid"]
ammonium_iodide = ["ammonium iodide"]
potassium_nitrate = ["potassium nitrate"]
bes = ["bes", "bes buffer"]
cesium_chloride = ["cscl"]
hexaamminecobalt = ["hexaamminecobalt"]
nh4h2po4 = ["nh4h2po4"]
oxonate = ["oxonate"]
potassium_formate = ["potassium formate"]
cocl2 = ["cocl2"]
ammonium_tartrate = ["ammonium tartrate"]
napi = ["napi"]
sodium_tartrate = ["sodium tartrate"]
ammonium_fluoride = ["ammonium fluoride"]
cobalt_chloride = ["cobalt chloride"]
magnesium_nitrate = ["magnesium nitrate"]

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
	matches = ANALYZE.find_chemical_matches(connection, "information", "id")
	# Split the chemicals into their concentrations, the units used to measure concentration,
	# and the name of the chemical itself
	split_matches = ANALYZE.split_results(matches[0], matches[1])
	# Store the results of the split_matches() function into a child table in the database
	SQL.delete_all_rows(connection, "crystallization_chemicals")
	ANALYZE.store_results(connection, "crystallization_chemicals", split_matches)
	# Display the frequency of each chemical name stored in the child table
	ANALYZE.create_common_chemical_names_list(connection, "crystallization_chemicals", "chemical_name")


def custom_search(local_base):
	connection = SQL.connect_database(local_base)
	# Find all of the chemicals stored in the crystallization information of the structures
	matches = ANALYZE.find_custom_crystallization_conditions(connection, "information", "id", non_ionic, ionic)


store_in_chemicals_table(BASE_DB)
# custom_search(BASE_DB)
