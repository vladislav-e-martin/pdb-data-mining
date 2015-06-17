# LIBRARIES AND PACKAGES

from ftplib import FTP
import os
import gzip
import glob
from pprint import pprint

# CONSTANTS

BASE = "/storage2/vlad/PDB"

# FUNCTION DECLARATIONS

def search(base, ftp):
    folders = ftp.nlst()
    # Iterate through each subdirectory containing all the protein structures
    for folder in folders:
        pprint ("Searching the " + folder + " directory.")

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
            pprint ("Reading the " + file[0] + " file.")
            if file[0].endswith(".gz"):
                #Store each file in the sub-directory created earlier
                dest_subdirectory = os.path.join(base, folder)
                dest_file = os.path.join(dest_subdirectory, file[0])
                decompressed_file = dest_file[:-3]
                # Check if the compressed file has already been decompressed into a XML file
                if os.path.isfile(decompressed_file):
                    pprint ("The file " + file[0][:-3] + " has already been downloaded.")
                    if os.path.isfile(dest_file):
                        pprint ("The compressed " + file[0] + " is now being removed.")
                        # Delete the compressed file to save space on the hard drive
                        os.remove(dest_file)
                # Download and decompress the file
                else:
                    pprint ("Downloading and extracting the " + file[0] + " file.")
                    ftp.retrbinary("RETR " + file[0], open(dest_file, 'wb').write)

                    src_subdirectory = os.path.join(base, folder)

                    for src_name in glob.glob(os.path.join(src_subdirectory, '*.gz')):
                        base = os.path.basename(src_name)
                        dest_name = os.path.join(dest_subdirectory, base[:-3])
                        with gzip.open(src_name, 'rb') as compressed:
                            with open(dest_name, 'wb') as decompressed:
                                for line in compressed:
                                    decompressed.write(line)
        ftp.cwd('../')

# Connect to FTP site
ftp = FTP("ftp.wwpdb.org")
ftp.login()

# Navigate to appropriate directory
ftp.cwd("/pub/pdb/data/structures/divided/XML/")

search(BASE, ftp)

ftp.quit()
