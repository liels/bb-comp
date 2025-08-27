import os
import sys

if len(sys.argv) != 2:
    print("Usage: python rename_script.py <target_directory>")
    sys.exit(1)

target_dir = sys.argv[1]

if not os.path.isdir(target_dir):
    print(f"Error: {target_dir} is not a valid directory.")
    sys.exit(1)

for filename in os.listdir(target_dir):
    if filename.endswith('.plot.tmp'):
        old_path = os.path.join(target_dir, filename)
        if os.path.isfile(old_path):  # Ensure it's a file
            new_filename = filename[:-4]  # Remove '.tmp'
            new_path = os.path.join(target_dir, new_filename)
            os.rename(old_path, new_path)
            print(f"Renamed: {filename} -> {new_filename}")
