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


def count_directories(ftp):
    folder_count = 0
    folders = ftp.nlst()
    # Iterate through each subdirectory containing all the protein structures
    for folder in folders:
        folder_count += 1
    return folder_count


# Download all files contained in each directory from the (start_shift)th directory to the (end_shift)th directory
#  from the wwPDB
def download_all(base, ftp):

    folders = ftp.nlst()
    # Iterate through each directory in the wwPDB
    for folder in folders:
        # Open the directory
        ftp.cwd(folder)

        # Store each sub-directory in it's own folder locally
        current_directory = os.path.join(base, folder)
        if not os.path.exists(current_directory):
            os.mkdir(current_directory)

        files = ftp.mlsd()
        # Iterate through each file contained in the current directory
        for file in [value for value in files if value is not None]:
            # Store each file in the sub-directory created earlier
            current_file_path = os.path.join(current_directory, file[0])
            decompressed_file_path = current_file_path[:-3]

            if file[0].endswith(".gz"):
                # Check if the compressed file has already been decompressed into a XML file
                if os.path.isfile(decompressed_file_path):
                    # pprint("The file " + file[0][:-3] + " has already been downloaded.")
                    if os.path.isfile(current_file_path):
                        pprint("The compressed " + file[0] + " is now being removed.")
                        # Delete the compressed file to save space on the hard drive
                        os.remove(current_file_path)
                # Download and decompress the file
                else:
                    pprint("Downloading and extracting the " + file[0] + " file.")
                    ftp.retrbinary("RETR " + file[0], open(current_file_path, 'wb').write)

                    for compressed_file_path in glob.glob(os.path.join(current_directory, '*.gz')):
                        compressed_filename = os.path.basename(compressed_file_path)
                        decompressed_filename = os.path.join(current_directory, compressed_filename[:-3])
                        with gzip.open(compressed_file_path, 'rb') as compressed:
                            with open(decompressed_filename, 'wb') as decompressed:
                                for line in compressed:
                                    decompressed.write(line)
        ftp.cwd('../')


# Download all files contained in each directory from the (start_shift)th directory to the (end_shift)th directory
#  from the wwPDB
def download_shift(base, ftp, start_shift, end_shift):
    current_shift = 0
    folders = ftp.nlst()
    # Iterate through each directory in the wwPDB
    for folder in folders:
        current_shift += 1
        pprint("Searching the " + folder + " directory.")

        # Wait until the (start_shift)th directory is reached to download files
        if current_shift < start_shift:
            continue
        if current_shift >= end_shift:
            break

        # Open the directory
        ftp.cwd(folder)

        # Store each sub-directory in it's own folder locally
        current_directory = os.path.join(base, folder)
        if not os.path.exists(current_directory):
            os.mkdir(current_directory)

        files = ftp.mlsd()
        # Iterate through each file contained in the current directory
        for file in [value for value in files if value is not None]:
            # Store each file in the sub-directory created earlier
            current_file_path = os.path.join(current_directory, file[0])
            decompressed_file_path = current_file_path[:-3]

            if file[0].endswith(".gz"):
                # Check if the compressed file has already been decompressed into a XML file
                if os.path.isfile(decompressed_file_path):
                    # pprint("The file " + file[0][:-3] + " has already been downloaded.")
                    if os.path.isfile(current_file_path):
                        pprint("The compressed " + file[0] + " is now being removed.")
                        # Delete the compressed file to save space on the hard drive
                        os.remove(current_file_path)
                # Download and decompress the file
                else:
                    pprint("Downloading and extracting the " + file[0] + " file.")
                    ftp.retrbinary("RETR " + file[0], open(current_file_path, 'wb').write)

                    for file_path in glob.glob(os.path.join(current_directory, '*.gz')):
                        compressed_filename = os.path.basename(file_path)
                        decompressed_filename = os.path.join(current_directory, compressed_filename[:-3])
                        with gzip.open(compressed_filename, 'rb') as compressed:
                            with open(decompressed_filename, 'wb') as decompressed:
                                for line in compressed:
                                    decompressed.write(line)
        ftp.cwd('../')


def disconnect(ftp):
    pprint("Disconnecting from the FTP site...")
    ftp.quit()
