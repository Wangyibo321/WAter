import os, json, math, random
import numpy as np

class WorkloadCompressor:
    def __init__(self, runner):
        self.runner = runner

    #########################################################
    ##### 1. Code related to initialize subset via GSUM #####
    #########################################################
    def get_GSUM_init_sql(self, workload_name, comp_ratio):
        gsum_init_sql_path = os.path.join("gsum_init_sql", f"{workload_name}", f"{workload_name}_{comp_ratio}.json")

        if not os.path.isfile(gsum_init_sql_path):
            print(f"{gsum_init_sql_path} doesn't exist.")
            return None
        
        with open(gsum_init_sql_path, 'r') as f:
            sql_dict = json.load(f)
        
        return sql_dict

    ################################################
    ##### 2. Code related to select new subset #####
    ################################################
    def select_queries(self):
        beta = 0.2
        time_budget = self.runner.comp_ratio * sum(self.runner.time_dict.values())
        comp_keys = []
        whole_query_keys = list(self.runner.whole_workload_queries.keys())
        print(f"whole_query_keys:{whole_query_keys}")

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
        
        new_subset  = {key: self.runner.whole_workload_queries[key] for key in comp_keys}
        print(f"selected_queries: {new_subset.keys()}")

        return new_subset

    def get_set_op(self, subset):
        if len(subset) == 0:
            print("RERTURN 0")
            return 0

        whole_time_lst = [sum([v1 for k1, v1 in v.items()]) for k, v in self.runner.single_dict["data"].items() if k in self.runner.exec_whole_idx]
        query_time_dict = {name:[v1[name] for k1, v1 in self.runner.single_dict["data"].items() if k1 in self.runner.exec_whole_idx] for name, _ in self.runner.whole_workload_queries.items()}
        # print(f"self.query_time_dict:{self.query_time_dict}")
        set_time_lst = np.sum(np.array([v for k, v in query_time_dict.items() if k in subset]), axis=0)
        # print(f"set_time_lst:{set_time_lst}")
        z, m = 0, 0
        for idx1 in range(len(set_time_lst)):
            for idx2 in range(len(whole_time_lst)):
                if idx1 != idx2:
                    if (whole_time_lst[idx1] < whole_time_lst[idx2] and set_time_lst[idx1] < set_time_lst[idx2]) or (whole_time_lst[idx1] > whole_time_lst[idx2] and set_time_lst[idx1] > set_time_lst[idx2]):
                        z += 1
                    m += 1
        op_value = z/(m+1e-9)
        return op_value

    def cost(self, keys):
        total_cost = 0
        for k in keys:
            total_cost += self.runner.time_dict[k]
        return total_cost

    def count_lack_round(self, q):
        data = self.runner.single_dict["data"]
        data = {k: v for k, v in sorted(data.items(), key=lambda x: int(x[0]))}
        round_to_run = 0
        for k, v in data.items():
            if q not in list(v.keys()):
                round_to_run += 1
        return round_to_run
    