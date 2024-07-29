import os

def delete_file(file_path):
    for file in [file_path, file_path+'.xz']:
        if os.path.exists(file):
            os.remove(file)
            