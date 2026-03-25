"""
Example usage

- Put your CSV files under a for example in folder named "C:/Desktop/data_feed"
- Run the generate_manifest.py script with python3 using this command with parameter as localization of files: "python3 generate_manifest.py C:/Desktop/data_feed"
- This script generates a file called "manifest" in the same folder with the CSV files.
"""

import os
import hashlib
import sys
from argparse import Namespace
from typing import Tuple, List

def hash_local_file(file_path, chunk_size):
    print(f"calculating checksum of {file_path} file")
    sha1 = hashlib.sha1()
    with open(file_path, mode="rb") as buffered_reader:
        chunk = buffered_reader.read(chunk_size)
        while chunk:
            sha1.update(chunk)
            chunk = buffered_reader.read(chunk_size)
    hash_result = sha1.hexdigest()
    print(f"checksum result: {hash_result}")
    return hash_result


def get_checksum(folder_path, file_names, chunk_size):
    print(f"calculating checksum of files in the {folder_path}")
    print(f"files: {file_names}")
    # create {file_name: checksum} mapping/dictionary
    checksum = {}
    for file_name in file_names:
        file_path = os.path.join(folder_path, file_name)
        file_checksum = hash_local_file(
            file_path=file_path,
            chunk_size=chunk_size,
        )
        checksum[file_name] = file_checksum
    print("calculated checksum results: ")
    print(sorted(checksum.items()))
    return checksum


def prepare_manifest_content(checksum: dict, delimiter: str) -> str:
    hash_algorithm = "SHA1"
    manifest = ""
    for file_name, sha1 in sorted(checksum.items()):
        manifest += f"{file_name}{delimiter}{hash_algorithm}{delimiter}{sha1}\n"
    return manifest


def get_files_and_directories(folder_path) -> Tuple[List[str], List[str]]:
    """
    Returns list of files with their full paths

    :param folder_path: command line arguments
    :type folder_path: Namespace

    :return: list of directories and file_names in the file tree
    :rtype: Tuple[List[str], List[str]]
    """
    file_names = []
    valid_suffixes = [".csv"] + ["manifest"]
    # if arguments.include_sub_dirs:
    for root, _, files in os.walk(folder_path):
        valid_files = [
            os.path.join(root, file).split(folder_path)[1]
            for file in files
            if file.lower().endswith(tuple(valid_suffixes))
        ]
        file_names.extend(valid_files)
    return None, file_names


def generate(folder_path):
    # ensure that folder_path ends with "/" character
    if not folder_path.endswith("/"):
        folder_path += "/"
    # get files and directories in the directory
    directories, file_names = get_files_and_directories(folder_path)
    # if a manifest file already exists in the directory,
    # remove it from the file list
    if "manifest" in file_names:
        print("manifest file already exists")
        file_names.remove("manifest")
    # calculate checksums
    checksum = get_checksum(
        folder_path=folder_path,
        file_names=file_names,
        chunk_size=10,
    )
    # generate manifest file content
    manifest = prepare_manifest_content(checksum=checksum, delimiter=" ")
    manifest_file_path = os.path.join(folder_path, "manifest")
    # if the manifest file already exists, overwrite it, otherwise create it.
    with open(manifest_file_path, "w", encoding="utf-8") as manifest_file:
        manifest_file.write(manifest)


if __name__ == "__main__":
    print("processing folder:"+sys.argv[1])
    generate(sys.argv[1])
