import random
import json
from faker import Faker
import time
from gateway import Gateway
from mapreduce import MapReduceEngine
import glob
import os
import numpy as np

# Init faker
fake = Faker('en_US')

# Consts
MIN_NUMBER_OF_WORDS_IN_FILE = 500
MAX_NUMBER_OF_WORDS_IN_FILE = 5 * 500
CHUNK_SIZE = 300 * 1000  # 300 kB


def create_small_files_stubs(number_of_files):
    total_data = []
    for i in range(number_of_files):
        number_of_words = random.randint(MIN_NUMBER_OF_WORDS_IN_FILE, MAX_NUMBER_OF_WORDS_IN_FILE)
        data = fake.sentence(nb_words=number_of_words, variable_nb_words=False)
        total_data.append(data)
        file_name = f"file_{i}_data.txt"
        metadata = {"filename": file_name, "size": len(data)}
        with open(f"metadata/file_{i}_meta.json", "w+") as metadata_file:
            json.dump(metadata, metadata_file)
        metadata_file.close()
        with open(f"files/{file_name}", "w+") as data_file:
            data_file.write(data)
        data_file.close()
    big_data = " ".join(total_data)
    with open(f"files/big_data.txt", "w+") as data_file:
        data_file.write(big_data)


def divide_files_to_batches(files):
    total_batch_size = 0
    current_batch = []
    batches = []
    for file in files:
        file_meta = Gateway.get_metadta(file)
        file_size = file_meta['size']
        if total_batch_size + file_size > CHUNK_SIZE:
            batches.append((current_batch.copy(), total_batch_size))
            current_batch.clear()
            total_batch_size = 0

        total_batch_size += file_size
        current_batch.append(file)

    if len(current_batch) > 0:
        batches.append((current_batch.copy(), total_batch_size))

    return batches


def map_reduce_appender(files):
    map_reduce_files = []
    batches = divide_files_to_batches(files)
    for idx, (batch, _) in enumerate(batches):
        files_data = Gateway.get_files(batch)
        new_data = " ".join(files_data[file] for file in batch)
        Gateway.write_file(f"tmp_{idx}", new_data)
        map_reduce_files.append(f"tmp_{idx}")
    engine = MapReduceEngine(map_reduce_files, word_count_map, word_count_reduce)
    engine.execute()
    Gateway.delete_files(map_reduce_files)


def map_reduce_multifile(files):
    batches = divide_files_to_batches(files)
    map_reduce_data = [batch[0] for batch in batches]
    engine = MapReduceEngine(map_reduce_data, word_count_multifile_map, word_count_reduce)
    engine.execute()


def word_count_map(filename, params):
    data = Gateway.get_file(filename)
    res = []
    words = data.split(' ')
    for word in words:
        res.append((word, 1))
    return res


def word_count_multifile_map(filenames, params):
    data = Gateway.get_files(filenames)
    res = []
    for filename in filenames:
        words = data[filename].split(' ')
        for word in words:
            res.append((word, 1))
    return res


def word_count_reduce(data, params):
    key, values = data
    reduce_value = len(values)
    res = [(key, reduce_value)]
    return res


def run_map_reduce_on_file(filename: str):
    engine = MapReduceEngine([filename], word_count_map, word_count_reduce)
    engine.execute()


def run_map_reduce_on_files(filenames: list):
    engine = MapReduceEngine(filenames, word_count_map, word_count_reduce)
    engine.execute()


def cleanup():
    files = glob.glob('output/*')
    for f in files:
        os.remove(f)


def files_cleanup():
    files = glob.glob('files/*')
    for f in files:
        os.remove(f)


def run_experiment(number_of_files):
    big_file_times = []
    small_files_times = []
    appender_times = []
    multi_file_times = []
    number_of_iterations = 50
    for i in range(number_of_iterations):
        create_small_files_stubs(number_of_files)

        t0 = time.time()
        run_map_reduce_on_file("big")
        t1 = time.time()
        big_file_times.append(t1 - t0)
        cleanup()

        t0 = time.time()
        files = [f"file_{i}" for i in range(number_of_files)]
        run_map_reduce_on_files(files)
        t1 = time.time()
        small_files_times.append(t1 - t0)
        cleanup()

        t0 = time.time()
        map_reduce_appender(files)
        t1 = time.time()
        appender_times.append(t1 - t0)
        cleanup()

        t0 = time.time()
        map_reduce_multifile(files)
        t1 = time.time()
        multi_file_times.append(t1 - t0)
        cleanup()
        print(f"Finished {i + 1} / {number_of_iterations}")
        files_cleanup()

    print(f"Results for {number_of_files} small files")
    print(f"Big files avg time: {np.mean(big_file_times)}")
    print(f"Small files avg time: {np.mean(small_files_times)}")
    print(f"Appender avg time: {np.mean(appender_times)}")
    print(f"Multi file avg time: {np.mean(multi_file_times)}")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run_experiment(50)
    run_experiment(100)
    run_experiment(200)
    run_experiment(500)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
