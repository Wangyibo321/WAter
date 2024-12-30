from abc import abstractmethod
import time
import sys
import os
import json
import threading
import glob
import re
import time
import functools
import multiprocessing
import concurrent.futures
from ConfigSpace import (
    ConfigurationSpace,
    UniformIntegerHyperparameter,
    UniformFloatHyperparameter,
    CategoricalHyperparameter,
)


class DefaultSpace:
    """ Base template of GPTuner"""
    def __init__(self, dbms, timeout, target_knobs_path,seed=1, workload_queries=None):
        self.dbms = dbms
        self.workload_queries = workload_queries
        self.seed = seed if seed is not None else 1
        self.timeout = timeout
        self.target_knobs_path = target_knobs_path
        self.round = 0
        self.summary_path = "./optimization_results/temp_results"
        self.benchmark_latency = ['tpch']
        self.search_space = ConfigurationSpace()
        self.skill_path = f"./knowledge_collection/{self.dbms.name}/structured_knowledge/normal"
        self.target_knobs = self.knob_select()
        self.log_file = f"./optimization_results/{self.dbms.name}/log/{self.seed}_log.txt"
        self.init_log_file()
        self.prev_end = 0
        self.record_single_path = f"./optimization_results/{self.dbms.name}/single/{self.seed}_single.json"
        # self.configspace_path = 
        


    def init_log_file(self):
        with open(self.log_file, 'w') as file:
            file.write(f"Round\tStart\tEnd\tBenchmark_Elapsed\tTuning_overhead\n")

    def _log(self, begin_time, end_time):
        if self.round == 1:
            self.prev_end = begin_time
        with open(self.log_file, 'a') as file:
            file.write(f"{self.round}\t{begin_time}\t{end_time}\t{end_time-begin_time}\t{begin_time-self.prev_end}\n")
        self.prev_end = end_time

    def _transfer_unit(self, value):
        value = str(value)
        value = value.replace(" ", "")
        value = value.replace(",", "")
        if value.isalpha():
            value = "1" + value
        pattern = r'(\d+\.\d+|\d+)([a-zA-Z]+)'
        match = re.match(pattern, value)
        if not match:
            return float(value)
        number, unit = match.group(1), match.group(2)
        unit_to_size = {
            'kB': 1e3,
            'KB': 1e3,
            'MB': 1e6,
            'GB': 1e9,
            'TB': 1e12,
            'K': 1e3,
            'M': 1e6,
            'G': 1e9,
            'B': 1,
            'ms': 1,
            's': 1000,
            'min': 60000,
            'day': 24 * 60 * 60000,
        }
        return float(number) * unit_to_size[unit]
    
    def _type_transfer(self, knob_type, value):
        value = str(value)
        value = value.replace(",", "")
        if knob_type == "integer":
            return int(round(float(value)))
        if knob_type == "real":
            return float(value)

    def knob_select(self):
        """ 
            Select which knobs to be tuned, store the names in 'self.target_knobs' 
            Default implementation is to use fixed knobs. Provide the path to the file containing the knobs' names.
        """
        current_directory = os.getcwd()
        print(current_directory)
        with open(self.target_knobs_path, 'r') as file:
            lines = file.readlines()
        candidate_knobs = [line.strip() for line in lines]
        target_knobs = []
        for knob in candidate_knobs:
            if "vartype" not in self.dbms.knob_info[knob] or self.dbms.knob_info[knob]["vartype"] == "string":
                continue
            else:
                target_knobs.append(knob)
        return target_knobs
    
    def get_default_space(self, knob_name, info):
        boot_value = info["reset_val"]
        min_value = info["min_val"]
        max_value = info["max_val"]
        knob_type = info["vartype"]
        if knob_type == "integer":
            if int(max_value) > sys.maxsize:
                knob = UniformIntegerHyperparameter(
                    knob_name, 
                    int(int(min_value) / 1000), 
                    int(int(max_value) / 1000),
                    default_value = int(int(boot_value) / 1000)
                )
            else:
                knob = UniformIntegerHyperparameter(
                    knob_name,
                    int(min_value),
                    int(max_value),
                    default_value = int(boot_value),
                )
        elif knob_type == "real":
            knob = UniformFloatHyperparameter(
                knob_name,
                float(min_value),
                float(max_value),
                default_value = float(boot_value)
            )
        elif knob_type == "enum":
            knob = CategoricalHyperparameter(
                knob_name,
                [str(enum_val) for enum_val in info["enumvals"]],
                default_value = str(boot_value),
            )
        elif knob_type == "bool":
            knob = CategoricalHyperparameter(
                knob_name,
                ["on", "off"],
                default_value = str(boot_value)
            )
        return knob

    def run_sql_with_timeout(self, timeout_seconds):
        result_queue = multiprocessing.Queue()

        def task_wrapper():
            print("HHH")
            result = self.run_sqls()
            result_queue.put(result)

        p = multiprocessing.Process(target=task_wrapper)
        p.start()
        p.join(timeout_seconds)

        if p.is_alive():
            p.terminate()
            p.join()
            if not result_queue.empty():
                sql_exec_time = result_queue.get()
            else:
                sql_exec_time = self.timeout
            return sql_exec_time
        else:
            return result_queue.get()
        
    def test(self, sql):
        dbms = self.dbms
        start_ms = time.time() * 1000.0
        flag = dbms.exec_queries(sql)
        end_ms = time.time() * 1000.0
        execution_time = end_ms - start_ms
        if flag: 
            return execution_time
        else:
            return self.timeout
        
    def run_sqls(self):
        start_run_time = time.time()
        dbms = self.dbms
        sql_exec_time = 0

        if os.path.exists(self.record_single_path):
            with open(self.record_single_path, 'r') as f:
                result = json.load(f)
        else:
            result = dict()
            result["data"], result["configs"] = dict(), dict()
        result["data"][self.round] = dict()
        result["configs"][self.round] = dbms.config

        for (j, sql) in self.workload_queries.items():
            if j in result["data"][self.round]:
                continue
            t = self.test(sql)
            print(f"TIME{j}:{t}")
            result["data"][self.round][j] = t
            sql_exec_time += t

        with open(self.record_single_path, 'w') as f:
            json.dump(result, f, indent=4)
            print(f"SAVE: {self.round}")
            print(self.record_single_path)
        actual_run_time = time.time() - start_run_time

        return sql_exec_time



    def set_and_replay(self, config, seed=0):
        begin_time = time.time()
        #cost = self.set_and_replay_ori(config, seed)
        total_sql_exec_time = self.set_and_replay_ori(config, seed)
        end_time = time.time()
        self._log(begin_time, end_time)
        #return cost
        return total_sql_exec_time


    def set_and_replay_ori(self, config, seed=0):
        dbms = self.dbms
        self.round += 1
        print(f"Tuning round {self.round} ...")
        print(f"--- Restore the dbms to default configuration ---")
        dbms.reset_config()
        dbms.reconfigure()

        for knob in self.target_knobs:
            try:
                control_para = config[f"control_{knob}"]
                if control_para == "0":
                    value = config[knob]       
                elif control_para == "1":
                    value = config[f"special_{knob}"]
            except:
                value = config[knob]
            dbms.set_knob(knob, value)
            
        if dbms.reconfigure():
            print("----- Executing workload on current configuration -----")
            total_sql_exec_time = self.run_sql_with_timeout(self.timeout)

            return total_sql_exec_time
        else:
            return self.timeout


    @abstractmethod
    def define_search_space(self):
        pass