from abc import ABC, abstractmethod
import time, os, json, re, multiprocessing
from WAter.workload_compression import WorkloadCompressor
from WAter.config_verification import ConfigVerifier
from WAter.history_reuse import HistoryReuser
import configparser

class RunnerTemplate(ABC):
    def __init__(self, tuner, dbms, timeout, target_knobs_path, seed, whole_workload_queries, workload_name):
        self.dbms = dbms
        self.seed = seed
        self.whole_workload_queries = {key: whole_workload_queries[key] for key in sorted(whole_workload_queries, key=lambda x: x[0])}
        self.time_dict_path = "./time_dict.json"
        self.record_single_path = ""
        self.ini_file_path = "./configs/water_params.ini"
        self.cur_stage = 0 # current time_slice
        self.round = 0 # number of finished round
        self.tuner = tuner
        self.cur_tuner = self.tuner[0]  # The tuner of coarse_stage is tuner[0] and the tuner of fine_stage is tuner[1]
        self.single_dict = {}
        self.exec_whole_idx = []
        self.exec_whole_last = []
        self.whole_time_lst = []
        self.cur_incumbent_cost, self.last_incumbent_cost = 1000000, 1000000
        self.target_knobs = self.cur_tuner.knob_select()
        self.num_knobs, self.cat_knobs = self.get_knob_type()
        self.timeout = timeout
        print(f"timeout: {self.timeout}")
        with open(self.time_dict_path, 'r') as f:
            self.time_dict = json.load(f)

        self.get_init_params()

        # initialize WorkloadCompressor
        self.compressor = WorkloadCompressor(self)
        self.cur_workload_queries = self.compressor.get_GSUM_init_sql(workload_name, self.comp_ratio)
        #self.cur_workload_queries = self.compressor.GPT_init_queries(self.comp_ratio)
        print(f"cur_workload_queries: {self.cur_workload_queries.keys()}")

        # initialize HistoryReuser
        self.history_reuser = HistoryReuser(self)

        # initialize ConfigVerifier
        self.verifier = ConfigVerifier(self)

    def get_init_params(self):
        config = configparser.ConfigParser()
        config.read(self.ini_file_path)

        self.tuning_budget_s = config.getint('WATER', 'tuning_budget_s')
        self.comp_ratio = config.getfloat('WATER', 'comp_ratio')
        self.verify_ratio = config.getfloat('WATER', 'verify_ratio')
        self.success_per_stage = config.getint('WATER', 'success_per_stage')
        self.update_threshold = config.getint('WATER', 'update_threshold')
        self.comp_ratio_add_unit = config.getfloat('WATER', 'comp_ratio_add_unit')

        print("--- WAter's hyperparameters have been initialized successfully ---")
        print(f"{'Parameter':<20}{'Value':<10}")
        print(f"{'-'*30}")
        print(f"{'tuning_budget_s':<20}{self.tuning_budget_s:<10}")
        print(f"{'comp_ratio':<20}{self.comp_ratio:<10}")
        print(f"{'verify_ratio':<20}{self.verify_ratio:<10}")
        print(f"{'success_per_stage':<20}{self.success_per_stage:<10}")
        print(f"{'update_threshold':<20}{self.update_threshold:<10}")
        print(f"{'comp_ratio_add_unit':<20}{self.comp_ratio_add_unit:<10}")
        print("------------------------------------------------------------------")

    def update_single_dict(self):
        with open(self.record_single_path, 'r') as f:
            self.single_dict = json.load(f)
            
    def dump_single_dict(self):
        with open(self.record_single_path, 'w') as f:
            json.dump(self.single_dict, f, indent=4)

    def get_knob_type(self):
        num_knobs = []
        cat_knobs = []
        for knob in self.target_knobs:
            info = self.dbms.knob_info[knob]
            if info is None:
                self.target_knobs.remove(knob)   # this knob is not by the DBMS under specific version
                continue
            
            knob_type = info["vartype"]
            if knob_type in ["enum", "bool"]:
                cat_knobs.append(knob)
            elif knob_type in ["integer", "real"]:
                num_knobs.append(knob)
        print(f"num_knobs: {num_knobs}")
        print(len(num_knobs))
        print(f"cat_knobs: {cat_knobs}")
        print(len(cat_knobs))
        quit()
        return num_knobs, cat_knobs

    def get_sql_time_with_timeout(self, sql, timeout_seconds):
        result_queue = multiprocessing.Queue()
        def task_wrapper(sql):
            result = self.get_sql_time(sql)
            print(f"RESULT:{result}")
            result_queue.put(result)

        p = multiprocessing.Process(target=task_wrapper, args=(sql,))
        p.start()
        p.join(timeout_seconds)
        if p.is_alive():
            p.terminate()
            p.join() 
            return self.timeout
        else:
            return result_queue.get()

    def get_sql_time(self, sql):
        dbms = self.dbms
        start_ms = time.time() * 1000.0
        flag = dbms.exec_queries(sql)
        end_ms = time.time() * 1000.0
        execution_time = end_ms - start_ms
        if flag: 
            return execution_time
        else:
            return self.timeout

    @abstractmethod
    def optimize(self):
        pass
