# LIBRARIES AND PACKAGES

from ftplib import FTP
import os
import gzip
import glob
from pprint import pprint

# CONSTANTS

# FUNCTION DECLARATIONS

# Connect to FTP site
def connect(base):
    ftp = FTP("ftp.wwpdb.org")
    ftp.login()
    pprint("Connected to the FTP site!")

    # Navigate to appropriate directory
    ftp.cwd(base)
    return ftp


def count_archives(ftp):
    folder_count = 0
    folders = ftp.nlst()
    # Iterate through each subdirectory containing all the protein structures
    for folder in folders:
        folder_count += 1
    return folder_count


# Download files from the wwPDB
def download(base, ftp, start_shift, end_shift):
    current_shift = 0
    folders = ftp.nlst()
    # Iterate through each subdirectory containing all the protein structures
    for folder in folders:
        current_shift += 1
        pprint("Searching the " + folder + " directory.")

        # Wait until it is time to start downloading from directories
        if (current_shift < start_shift):
            pprint("Skipping this directory...")
            continue
        if (current_shift >= end_shift):
            break

        # Open the directory
        ftp.cwd(folder)

        # Store each sub-directory in it's own folder locally
        try:
            os.mkdir(os.path.join(base, folder))
        except:
            pass

        files = ftp.mlsd()
        # Iterate through each file in the current subdirectory
        for file in [value for value in files if value is not None]:
            pprint("Reading the " + file[0] + " file.")
            # Store each file in the sub-directory created earlier
            dest_subdirectory = os.path.join(base, folder)
            dest_file = os.path.join(dest_subdirectory, file[0])
            decompressed_file = dest_file[:-3]

            if file[0].endswith(".gz"):
                # Check if the compressed file has already been decompressed into a XML file
                if os.path.isfile(decompressed_file):
                    pprint("The file " + file[0][:-3] + " has already been downloaded.")
                    if os.path.isfile(dest_file):
                        pprint("The compressed " + file[0] + " is now being removed.")
                        # Delete the compressed file to save space on the hard drive
                        os.remove(dest_file)
                # Download and decompress the file
                else:
                    pprint("Downloading and extracting the " + file[0] + " file.")
                    ftp.retrbinary("RETR " + file[0], open(dest_file, 'wb').write)

                    src_subdirectory = os.path.join(base, folder)

                    for src_name in glob.glob(os.path.join(src_subdirectory, '*.gz')):
                        base_name = os.path.basename(src_name)
                        dest_name = os.path.join(dest_subdirectory, base_name[:-3])
                        with gzip.open(src_name, 'rb') as compressed:
                            with open(dest_name, 'wb') as decompressed:
                                for line in compressed:
                                    decompressed.write(line)
        ftp.cwd('../')


def download_specific_files(base, ftp, specified_ids):
    folders = ftp.nlst()
    # Iterate through each subdirectory containing all the protein structures
    for folder in folders:
        pprint("Searching the " + folder + " directory.")

        # Open the directory
        ftp.cwd(folder)

        # Store each sub-directory in it's own folder locally
        try:
            os.mkdir(os.path.join(base, folder))
        except:
            pass

        files = ftp.mlsd()
        # Iterate through each file in the current subdirectory
        for file in [value for value in files if value is not None]:
            pprint("Reading the " + file[0] + " file.")
            # Store each file in the sub-directory created earlier
            dest_subdirectory = os.path.join(base, folder)
            dest_file = os.path.join(dest_subdirectory, file[0])
            decompressed_file = dest_file[:-3]
            # For the XML files containing ALL information (i.e., including atom coordinates)
            entry_id = decompressed_file[:-4]

            if entry_id in specified_ids:
                if file[0].endswith(".gz"):
                    # Check if the compressed file has already been decompressed into a XML file
                    if os.path.isfile(decompressed_file):
                        pprint("The file " + file[0][:-3] + " has already been downloaded.")
                        if os.path.isfile(dest_file):
                            pprint("The compressed " + file[0] + " is now being removed.")
                            # Delete the compressed file to save space on the hard drive
                            os.remove(dest_file)
                    # Download and decompress the file
                    else:
                        pprint("Downloading and extracting the " + file[0] + " file.")
                        ftp.retrbinary("RETR " + file[0], open(dest_file, 'wb').write)

                        src_subdirectory = os.path.join(base, folder)

                        for src_name in glob.glob(os.path.join(src_subdirectory, '*.gz')):
                            base_name = os.path.basename(src_name)
                            dest_name = os.path.join(dest_subdirectory, base_name[:-3])
                            with gzip.open(src_name, 'rb') as compressed:
                                with open(dest_name, 'wb') as decompressed:
                                    for line in compressed:
                                        decompressed.write(line)
        ftp.cwd('../')


def disconnect(ftp):
    pprint("Disconnecting from the FTP site...")
    ftp.quit()
