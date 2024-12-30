import time
import os, json
from abc import ABC, abstractmethod
import json
import os
import re
import multiprocessing
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
import numpy as np
from util import *
import random

class RunnerTemplate(ABC):
    def __init__(self, tuner, dbms, timeout, target_knobs_path, seed, whole_workload_queries):
        self.dbms = dbms
        self.seed = seed
        self.whole_workload_queries = {key: whole_workload_queries[key] for key in sorted(whole_workload_queries, key=lambda x: x[0])}
        self.time_dict_path = "./time_dict.json"
        self.timeout = timeout
        try:
            with open(self.time_dict_path, 'r') as f:
                self.time_dict = json.load(f)
        except:
            self.get_time_dict()
            

        self.comp_ratio = self.init_ratio()
        self.verify_ratio = 0.3
        self.success_per_stage = 15 # 每个阶段成功多少轮才进入下一个阶段
        self.update_threshold = 1 # 几个阶段都没有更新incumbent就增大压缩比例
        self.cur_stage = 0
        self.tuning_budget_s = 15000 # 调优时间
        self.round = 0 # 已完成的round
        self.cur_workload_queries = self.init_queries()
        self.tuner = tuner
        self.exec_whole_idx = []
        self.whole_time_lst = []
        self.query_time_dict = {}
        self.single_dict = {}
        self.cur_incumbent_cost, self.last_incumbent_cost = 1000000, 1000000
        self.num_knobs, self.cat_knobs = self.get_knob_type()
        self.rf = self.get_rf()
        self.exec_whole_last = []
        self.record_single_path = ""
        self.cur_tuner = self.tuner[0]
        with open(target_knobs_path, 'r') as file:
            lines = file.readlines()
        self.target_knobs = self.cur_tuner.knob_select()
        
    def init_ratio(self):
        return 0.3
    
    def init_queries(self):    
        acc_time = 0
        time_budget = self.comp_ratio * sum(self.time_dict.values())
        
        top_keys = []
        for key, value in self.time_dict.items():
            if value + acc_time > time_budget:
                continue
            else:
                acc_time += value
                top_keys.append(key)
        return {key: self.whole_workload_queries[key] for key in top_keys}


    def get_time_dict(self):
        dbms = self.dbms
        dbms.reset_config()
        dbms.reconfigure()
        self.time_dict = dict()
        for name, sql in self.whole_workload_queries.items():
            start_time = time.time()
            dbms.exec_queries(sql)
            t = time.time() - start_time
            self.time_dict[name] = t
        with open(self.time_dict_path, 'w') as f:
            json.dump(self.time_dict, f, indent=4)
            
    def get_knob_type(self):
        num_knobs = []
        cat_knobs = []
        train_data = {}
        with open("./optimization_results/configspace.json", 'r') as f:
            para = json.load(f)["hyperparameters"]
        for i in para:
            if "control_" not in i["name"] and "special_" not in i["name"]:
                if "int" in i["type"] or "float" in i["type"]:
                    num_knobs.append(i["name"])
                else:
                    cat_knobs.append(i["name"])
        return num_knobs, cat_knobs
            
    def get_rf(self):
        return RandomForestRegressor(n_estimators=200)
        
    def get_rf_train_data(self):
        self.update_single_dict()
        
        X = {knob:[] for knob in self.target_knobs}
        Y = {"cost":[]}
        self.update_single_dict()
        
        for i in self.exec_whole_idx:
            for knob in self.target_knobs:
                try:
                    control_para = self.single_dict["configs"][i][f"control_{knob}"]
                    if control_para == "0":
                        value = self.single_dict["configs"][i][knob]       
                    elif control_para == "1":
                        value = self.single_dict["configs"][i][f"special_{knob}"]
                except:
                    value = self.single_dict["configs"][i][knob]
                X[knob].append(value)
                
            Y["cost"].append(sum(list(self.single_dict["data"][str(i)].values())))    
        X = pd.DataFrame(X)
        Y = pd.DataFrame(Y)
        self.get_preprocessor()
        X = self.preprocessor.transform(X)
        X = X.astype(np.float32)
        X = np.clip(X, np.finfo(np.float32).min, np.finfo(np.float32).max)
        
        return X, Y

    def get_rf_predict_data(self):
        self.update_single_dict()
        X = {knob:[] for knob in self.target_knobs}
        Y = {"cost":[]}
        
        for i in self.success_run_last:
            for knob in self.target_knobs:
                try:
                    control_para = self.single_dict["configs"][i][f"control_{knob}"]
                    if control_para == "0":
                        value = self.single_dict["configs"][i][knob]       
                    elif control_para == "1":
                        value = self.single_dict["configs"][i][f"special_{knob}"]
                except:
                    value = self.single_dict["configs"][i][knob]
                X[knob].append(value)
                
            Y["cost"].append(sum(list(self.single_dict["data"][str(i)].values())))    
        X = pd.DataFrame(X)
        Y = pd.DataFrame(Y)
        self.get_preprocessor()
        X = self.preprocessor.transform(X)
        X = X.astype(np.float32)
        X = np.clip(X, np.finfo(np.float32).min, np.finfo(np.float32).max)
        
        return X, Y
    
    def get_preprocessor(self):
        x_known, x_unknown = self.get_raw_X()
        X = pd.concat([x_known, x_unknown], axis=0)
        self.preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), self.num_knobs),
                ('cat', OneHotEncoder(), self.cat_knobs)
            ])
        self.preprocessor.fit(X)
    
    def get_raw_X(self):
        self.update_single_dict()
        X_known = {knob:[] for knob in self.target_knobs}
        X_unknown = {knob:[] for knob in self.target_knobs}
        Y = {"cost":[]}
        
        for i in self.exec_whole_idx: #已知的数据
            for knob in self.target_knobs:
                try:
                    control_para = self.single_dict["configs"][i][f"control_{knob}"]
                    if control_para == "0":
                        value = self.single_dict["configs"][i][knob]       
                    elif control_para == "1":
                        value = self.single_dict["configs"][i][f"special_{knob}"]
                except:
                    value = self.single_dict["configs"][i][knob]
                X_known[knob].append(value)
                
            Y["cost"].append(sum(list(self.single_dict["data"][str(i)].values())))
        X_known = pd.DataFrame(X_known)
        
        ###
        for i in self.success_run_last: # 要预测的数据
            for knob in self.target_knobs:
                try:
                    control_para = self.single_dict["configs"][i][f"control_{knob}"]
                    if control_para == "0":
                        value = self.single_dict["configs"][i][knob]       
                    elif control_para == "1":
                        value = self.single_dict["configs"][i][f"special_{knob}"]
                except:
                    value = self.single_dict["configs"][i][knob]
                X_unknown[knob].append(value)
                
            Y["cost"].append(sum(list(self.single_dict["data"][str(i)].values())))    
        X_unknown = pd.DataFrame(X_unknown)

        return X_known, X_unknown
        
    def rf_update(self):
        X, Y = self.get_rf_train_data()
        self.rf.fit(X, Y["cost"])
        
    def uncertainty_scores(self, X):
        all_predictions = np.zeros((X.shape[0], len(self.rf.estimators_)))
        for i, estimator in enumerate(self.rf.estimators_):
            all_predictions[:, i] = estimator.predict(X)
        uncertainty = np.std(all_predictions, axis=1)
        return uncertainty
    
    def similarity_func(self, x1, x2, min_values, max_values):
        n_features = len(x1)
        sum_dist = 0.0

        for i in range(n_features):
            if isinstance(x1[i], (int, float)) and isinstance(x2[i], (int, float)):  # Continuous variable
                sum_dist += abs(x1[i] - x2[i]) / (max_values[i] - min_values[i])
            else:  # Categorical variable
                sum_dist += 0 if x1[i] == x2[i] else 1

        return  n_features / (sum_dist + n_features)
    
    def set_similarity(self, X_known, X_unknown):
        # X_known, X_unknown = self.get_raw_X()
        min_values = X_known[self.num_knobs].min()
        max_values = X_known[self.num_knobs].max()
        sims = []
        for i in range(X_unknown.shape[0]):
            biggest_sim = 0.0
            for j in range(X_known.shape[0]):
                sim = self.similarity_func(X_unknown.iloc[i], X_known.iloc[j], min_values, max_values)
                biggest_sim = max(sim, biggest_sim)
            sims.append(biggest_sim)

        return sims
        
    def top_op_queries(self):
        # 按op值排序后，所有的sql扫一次，若加入此sql，后时间和不超过ratio*总时间就加入，否则不加入此sql。
        self.compute_single_op()
        acc_time = 0
        time_budget = self.comp_ratio * sum(self.time_dict.values())
        
        top_keys = []
        for key, _ in self.op_dict.items():
            if self.time_dict[key] + acc_time > time_budget:
                continue
            else:
                acc_time += self.time_dict[key]
                top_keys.append(key)
        return {key: self.whole_workload_queries[key] for key in top_keys}    

    def compute_single_op(self):
        """单个sql的op值"""
        self.update_single_dict()
        self.whole_time_lst = [sum([v1 for k1, v1 in v.items()]) for k, v in self.single_dict["data"].items() if k in self.exec_whole_idx]
        self.query_time_dict = {name:[v1[name] for k1, v1 in self.single_dict["data"].items() if k1 in self.exec_whole_idx] for name, _ in self.whole_workload_queries.items()}
        self.op_dict = {}
        for name, query_time in self.query_time_dict.items():
            z, m = 0, 0
            print(len(query_time), len(self.whole_time_lst))
            assert len(query_time) == len(self.whole_time_lst)
            for idx1 in range(len(query_time)):
                for idx2 in range(len(self.whole_time_lst)):
                    if idx1 != idx2:
                        if (self.whole_time_lst[idx1] < self.whole_time_lst[idx2] and query_time[idx1] < query_time[idx2]) or (self.whole_time_lst[idx1] > self.whole_time_lst[idx2] and query_time[idx1] > query_time[idx2]):
                            z += 1
                        m += 1
            self.op_dict[name] = z/(m+1e-6)
        self.op_dict = dict(sorted(self.op_dict.items(), key=lambda x: x[1], reverse=True))
        return self.op_dict
    
    def get_set_op(self, sub_set):
        """集合的op值"""
        if len(sub_set) == 0:
            print("RERTURN 0")
            return 0
        # print(f"sub_set:{sub_set}")
        self.update_single_dict()
        self.whole_time_lst = [sum([v1 for k1, v1 in v.items()]) for k, v in self.single_dict["data"].items() if k in self.exec_whole_idx]
        self.query_time_dict = {name:[v1[name] for k1, v1 in self.single_dict["data"].items() if k1 in self.exec_whole_idx] for name, _ in self.whole_workload_queries.items()}
        # print(f"self.query_time_dict:{self.query_time_dict}")
        set_time_lst = np.sum(np.array([v for k, v in self.query_time_dict.items() if k in sub_set]), axis=0)
        # print(f"set_time_lst:{set_time_lst}")
        z, m = 0, 0
        for idx1 in range(len(set_time_lst)):
            for idx2 in range(len(self.whole_time_lst)):
                if idx1 != idx2:
                    if (self.whole_time_lst[idx1] < self.whole_time_lst[idx2] and set_time_lst[idx1] < set_time_lst[idx2]) or (self.whole_time_lst[idx1] > self.whole_time_lst[idx2] and set_time_lst[idx1] > set_time_lst[idx2]):
                        z += 1
                    m += 1
        op_value = z/(m+1e-9)
        return op_value
    
    def select_queries(self):
        # 直接选择单个op值最高的sql
        # self.cur_workload_queries = self.top_op_queries()
        # self.cur_tuner.workload_queries = self.cur_workload_queries
        # print(f"selected_queries:{self.cur_workload_queries.keys()}")
        
        # greedy algorithm
        beta = 0.2
        time_budget = self.comp_ratio * sum(self.time_dict.values())
        comp_keys = []
        whole_query_keys = list(self.whole_workload_queries.keys())
        print(f"whole_query_keys:{whole_query_keys}")
        
        # delta = {q: -float('inf') for q in whole_query_keys}
        delta = {q: self.get_set_op([q]) for q in whole_query_keys}
        while len(whole_query_keys) > 0:
            delta_star = -float('inf')
            q_star = None
            
            M1, m1, M2, m2 = -float('inf'), float('inf'), -float('inf'), float('inf')
            for q in whole_query_keys:
                val = (self.get_set_op(comp_keys + [q]) - self.get_set_op(comp_keys)) / self.cost([q])
                lack_round = self.count_lack_round(q)
                print(f"val:{val}")
                print(f"self.get_set_op:{self.get_set_op(comp_keys + [q])}")
                print(f"get_set_op:{self.get_set_op(comp_keys)}")
                print(q)
                M1 = max(M1, val)
                m1 = min(m1, val)
                M2 = max(M2, lack_round)
                m2 = min(m2, lack_round)
                
            print(m1, M1, m2, M2)
            for q in whole_query_keys:
                print(self.cost([q]))
                if delta[q] >= delta_star:
                    delta[q] = (self.get_set_op(comp_keys + [q]) - self.get_set_op(comp_keys) - self.cost([q]) * m1) / (self.cost([q]) * (M1-m1+0.001)) - beta * (self.count_lack_round(q) - m2) / (M2-m2+0.001)
                    
                if delta[q] >= delta_star:
                    delta_star = delta[q]
                    q_star = q
                    
            if self.cost(comp_keys) + self.cost([q_star]) <= time_budget:
                comp_keys.append(q_star)
            
            whole_query_keys.remove(q_star)
            
        self.cur_workload_queries  = {key: self.whole_workload_queries[key] for key in comp_keys}
        self.cur_tuner.workload_queries = self.cur_workload_queries
        print(f"selected_queries:{self.cur_workload_queries.keys()}")
        
        return self.cur_workload_queries 
    
    
    def count_lack_round(self, q):
        self.update_single_dict()
        data = self.single_dict["data"]
        data = {k: v for k, v in sorted(data.items(), key=lambda x: int(x[0]))}
        round_to_run = 0
        for k, v in data.items():
            if q not in list(v.keys()):
                round_to_run += 1
                
        return round_to_run
        
    def cost(self, keys):
        total_cost = 0
        for k in keys:
            total_cost += self.time_dict[k]
        return total_cost
        
    def select_round_to_run(self):
        self.update_single_dict()
        epsilon = 0.5
        if random.random() <= epsilon:
            self.rf_update()
            X, _ = self.get_rf_predict_data()
            rf_score = self.rf.predict(X)
            # rf 预测的和子集的latency加权
            with open(self.cur_tuner.runhistory_path, 'r') as f:
                runhistory = json.load(f)
            whole_score = {idx:score*(1 - self.comp_ratio) for idx, score in zip(self.success_run_last, rf_score)}
            for i in self.success_run_last:
                whole_score[i] += runhistory["data"][int(i)-1][4] * self.comp_ratio
                
                
            whole_score = {k:-v for k, v in whole_score.items()} # latency, the less the better
            print(f"whole_score1:{whole_score}")

        else:
            X_known, X_unknown = self.get_raw_X()
            sim_scores = self.set_similarity(X_known, X_unknown)
            
            self.rf_update()
            X, _ = self.get_rf_predict_data()
            uncertainty_scores = self.uncertainty_scores(X)
            
            r = len(X_unknown)/(len(X_known) + len(X_unknown))
            whole_score = {idx:r*(1 - sim_score)+(1-r)*uncertainty_score for idx, sim_score, uncertainty_score in zip(self.success_run_last, sim_scores, uncertainty_scores)}
            print(f"whole_score:{whole_score}")
            
        self.exec_whole_last = sorted(whole_score, key=whole_score.get)[:int(self.success_per_stage * self.verify_ratio)]
        self.exec_whole_idx.extend(self.exec_whole_last)
            
        
    def exec_whole(self):
        """执行选中要执行全部sql的轮次, 返回更新后的single_dict"""
        self.update_single_dict()
        whole_to_remove = []  # 删除超时的轮次
        for i in self.exec_whole_last:
            print(f"exec_whole:{i}")
            dbms = self.dbms
            configs = self.single_dict["configs"][str(i)]
            dbms.set_config(configs)
            for name, sql in self.whole_workload_queries.items():
                dbms = self.dbms
                if name not in self.single_dict["data"][str(i)]:
                    print(f"TEST SINGLE WHOLE {name}")
                    timeout_seconds = self.timeout - sum(list(self.single_dict["data"][str(i)].values()))/1e3
                    timeout_seconds = max(0, timeout_seconds)
                    print(f"TIME OUT:{timeout_seconds}")
                    t = self.get_sql_time_with_timeout(sql, dbms, timeout_seconds)
                    if t == 1000000:  # 超时的直接删除
                        if str(i) in self.single_dict["data"]:
                                del self.single_dict["data"][str(i)]
                        whole_to_remove.append(i)
                        print("time out")
                        break
                    else:
                        self.single_dict["data"][str(i)][name] = t
                        

            if "cost" not in self.single_dict:
                self.single_dict["cost"] = []
            if str(i) in self.single_dict["data"]:
                self.single_dict["cost"].append({"round":str(i), "time": time.time()- self.start_time, "cost":sum(list(self.single_dict["data"][str(i)].values()))})
            self.dump_single_dict()
            
        for i in whole_to_remove:
            self.exec_whole_last.remove(i)
            self.exec_whole_idx.remove(i)
            
        self.dump_single_dict()


    def exec_selected_on_history(self):
        self.update_single_dict()
        # whole_to_remove = []
        for i in range(1, self.round+1):
            dbms = self.dbms
            if str(i) in self.single_dict["data"].keys() and not set(self.cur_workload_queries.keys()).issubset(set(self.single_dict["data"][str(i)].keys())):

                configs = self.single_dict["configs"][str(i)]
                dbms.set_config(configs)
            
                for name, sql in self.cur_workload_queries.items():
                    dbms = self.dbms
                    if name not in self.single_dict["data"][str(i)]:
                        print(f"TEST SINGLE S {name}")
                        timeout_seconds = self.timeout - sum([v for k, v in self.single_dict["data"][str(i)].items()])/1e3
                        timeout_seconds = max(timeout_seconds, 0)
                        print(f"TIME OUT:{timeout_seconds}")
                        t = self.get_sql_time_with_timeout(sql, dbms, timeout_seconds)
                        print(f"SINGLE TIME:{t}")

                        if t == 1000000:  # time out
                            if str(i) in self.single_dict["data"]:
                                del self.single_dict["data"][str(i)]
                            # whole_to_remove.append(i)
                            print("time out")
                            break
                        else:
                            self.single_dict["data"][str(i)][name] = t
        self.dump_single_dict()

    def update_single_dict(self):
        with open(self.record_single_path, 'r') as f:
            self.single_dict = json.load(f)
            
    def dump_single_dict(self):
        with open(self.record_single_path, 'w') as f:
            json.dump(self.single_dict, f, indent=4) 
        
    def get_sql_time_with_timeout(self, sql, dbms, timeout_seconds):
        result_queue = multiprocessing.Queue()
        def task_wrapper(sql, dbms):
            result = self.get_sql_time(sql, dbms)
            print(f"RESULT:{result}")
            result_queue.put(result)

        p = multiprocessing.Process(target=task_wrapper, args=(sql, dbms))
        p.start()
        p.join(timeout_seconds)
        if p.is_alive():
            p.terminate()
            p.join() 
            return 1000000 
        else:
            return result_queue.get()

    def get_sql_time(self, sql, dbms):
        start_ms = time.time() * 1000.0
        flag = dbms.exec_queries(sql)
        end_ms = time.time() * 1000.0
        execution_time = end_ms - start_ms
        if flag: 
            return execution_time
        else:
            return 1000000
        
        
    
    @abstractmethod
    def optimize(self):
        pass
        