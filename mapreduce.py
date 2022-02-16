import threading
import csv
import pandas as pd
import sqlite3
import os
import glob


class MapReduceJob(threading.Thread):
    def __init__(self, thread_id, function, data, params):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.function = function
        self.data = data
        self.params = params

    def run(self):
        # print(f"Starting {self.name} job {self.thread_id}")
        results = self.function(self.data, self.params)

        # write output to csv file
        output_csv_headers = ["key", "value"]
        with open(self.filename, 'w+', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(output_csv_headers)
            for result in results:
                row = [result[0], result[1]]
                writer.writerow(row)
        f.close()
        # print(f"Finished {self.name} job {self.thread_id}")

    @property
    def filename(self):
        return ''


class MapJob(MapReduceJob):
    @property
    def filename(self):
        return f"mapreducetemp/part-tmp-{self.thread_id}.csv"

    @property
    def name(self):
        return "map"


class ReduceJob(MapReduceJob):
    @property
    def filename(self):
        return f"output/part-{self.thread_id}-final.csv"

    @property
    def name(self):
        return "map"


class MapReduceEngine:
    # input_data: Array of elements to process
    # map_function: The mapping function. (data, params) -> list({key, value})
    # reduce_function The reduce function. (key, value) -> list({result_key, result_value})
    # params parameters to the map function, in form of {key:value}
    def __init__(self, input_data, map_function, reduce_function, params=None):
        self.input_data = input_data
        self.map_function = map_function
        self.reduce_function = reduce_function
        self.params = params
        self.db_name = 'mydb.db'

    def execute(self):
        self.setup()
        self.mapping()
        self.reducing()
        self.cleanup()
        # print("MapReduce Completed")

    def setup(self):
        connection = sqlite3.connect(self.db_name)
        cur = connection.cursor()
        cur.execute('''CREATE TABLE temp_results(key TEXT,value TEXT)''')
        connection.commit()
        connection.close()

    def mapping(self):
        mapping_jobs = []
        for idx, data in enumerate(self.input_data):
            job = MapJob(idx, self.map_function, data, self.params)
            job.start()
            mapping_jobs.append(job)

        # Wait for mapping to finish
        self.wait_for_all(mapping_jobs)

        # Gather all of the csv
        connection, cur = self.connect_to_db()
        for job in mapping_jobs:
            data = pd.read_csv(job.filename)
            data.to_sql('temp_results', connection, if_exists='append', index=False)
        connection.close()

    def reducing(self):
        connection, cur = self.connect_to_db()
        reduce_jobs = []
        index = 0
        for row in cur.execute('''
                        SELECT key,GROUP_CONCAT(value) 
                        FROM temp_results 
                        GROUP BY key 
                        ORDER BY key;
                        '''):
            key = row[0]
            values = row[1].split(',')
            job = ReduceJob(index, self.reduce_function, (key, values), self.params)
            job.start()
            reduce_jobs.append(job)
            index += 1

        # Wait for mapping to finish
        self.wait_for_all(reduce_jobs)
        connection.close()

    def cleanup(self):
        os.remove(self.db_name)
        files = glob.glob('mapreducetemp/*')
        for f in files:
            os.remove(f)

    def connect_to_db(self):
        connection = sqlite3.connect(self.db_name)
        cur = connection.cursor()
        return connection, cur

    @staticmethod
    def wait_for_all(jobs):
        for job in jobs:
            job.join()
