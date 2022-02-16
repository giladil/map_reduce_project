import json
import os


class Gateway:
    @staticmethod
    def get_file(filename: str):
        file = open(f"files/{filename}_data.txt", "r")
        data = file.read()
        file.close()
        return data

    @staticmethod
    def write_file(filename: str, data):
        with open(f"files/{filename}_data.txt", "w+") as data_file:
            data_file.write(data)
        data_file.close()

    @staticmethod
    def get_files(filenames: list):
        files_data = {}
        for filename in filenames:
            file = open(f"files/{filename}_data.txt", "r")
            data = file.read()
            file.close()
            files_data[filename] = data
        return files_data

    @staticmethod
    def get_metadta(filename: str):
        file = open(f"metadata/{filename}_meta.json", "r")
        json_data = json.load(file)
        file.close()
        return json_data

    @staticmethod
    def delete_files(filenames: list):
        for filename in filenames:
            os.remove(f"files/{filename}_data.txt")
