import logging
import os
import requests
import lzma


def download_file(url, file_path):
    logging.info(url)
    file_path_xz = file_path + ".xz"

    response = requests.get(url)
    response.raise_for_status()

    with open(file_path_xz, 'wb') as file:
        file.write(response.content)

    with lzma.open(file_path_xz, 'rt', encoding='utf-8') as lzr, \
            open(file_path, 'wt') as fp:
        data = lzr.read()
        fp.write(data)

    logging.info(f"File {file_path} loaded successfully, it has size {os.path.getsize(file_path)}")
