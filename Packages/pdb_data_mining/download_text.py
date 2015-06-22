__author__ = 'Vlad'

# LIBRARIES AND PACKAGES

from ftplib import FTP
import os
import gzip
import glob
from pprint import pprint

# CONSTANTS

BASE = "/storage2/vlad/PDB/text/"
XML = "/pub/pdb/data/structures/divided/XML/"
TEXT = "/pub/pdb/data/structures/divided/pdb/"

# FUNCTION DECLARATIONS

# Connect to FTP site
def connect(base):
	ftp = FTP("ftp.wwpdb.org")
	ftp.login()
	pprint("Connected to the FTP site!")

	# Navigate to appropriate directory
	ftp.cwd(TEXT)
	return ftp


def count_archives(base, ftp):
	file_count = 0
	folders = ftp.nlst()
	# Iterate through each subdirectory containing all the protein structures
	for folder in folders:
		pprint("Searching the " + folder + " directory.")

		# Open the directory
		ftp.cwd(folder)

		files = ftp.mlsd()
		# Iterate through each file in the current subdirectory
		for file in [value for value in files if value is not None]:
			file_count += 1
		ftp.cwd('../')
	return file_count


# Search the FTP site
def search(base, ftp, start_shift, end_shift):
	current_period = 0
	folders = ftp.nlst()
	# Iterate through each subdirectory containing all the protein structures
	for folder in folders:
		pprint("Searching the " + folder + " directory.")

		# Wait until it is time to start downloading from directories
		if (current_period < start_shift):
			continue
		if (current_period >= end_shift):
			continue

		# Open the directory
		ftp.cwd(folder)

		# Store each sub-directory in it's own folder locally
		try:
			os.mkdir(base + folder)
		except:
			pass

		files = ftp.mlsd()
		# Iterate through each file in the current subdirectory
		for file in [value for value in files if value is not None]:
			pprint("Reading the " + file[0] + " file.")
			# Store each file in the sub-directory created earlier
			dest_subdirectory = os.path.join(base, folder)
			dest_file = os.path.join(dest_subdirectory, file[0])
			pprint("The file's destination directory: {0}".format(dest_file))
			# Check if the file has already been downloaded
			if os.path.isfile(file):
				pprint("The file " + file[0] + " has already been downloaded.")
			# Download the file
			else:
				pprint("Downloading and extracting the " + file[0] + " file.")
				ftp.retrbinary("RETR " + file[0], open(dest_file, 'wb').write)
		ftp.cwd('../')
		current_period += 1


def disconnect(BASE, ftp):
	pprint("Disconnecting from the FTP site...")
	ftp.quit()
