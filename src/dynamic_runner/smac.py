import sys
import os
import json
import re
import time
from dynamic_runner.runner_template import RunnerTemplate
from config_recommender.coarse_stage import CoarseStage
from config_recommender.fine_stage import FineStage
from smac.runhistory.dataclasses import TrialValue

class RunnerSMAC(RunnerTemplate):
    def __init__(self, tuner, dbms, timeout, target_knobs_path, seed, whole_workload_queries):
        super().__init__(tuner, dbms, timeout, target_knobs_path, seed, whole_workload_queries)
        self.cur_tuner = self.tuner[0]
        self.record_single_path = self.cur_tuner.record_single_path
        """"coarse和fine 两个tuner分别是tuner[0], tuner[1]"""
        
    def optimize(self):
        self.start_time = time.time()

        # 连续几轮没变化就增加压缩率
        stage_to_run = self.update_threshold
        while True:
            self.cur_stage += 1
            self.cur_tuner.workload_queries = self.cur_workload_queries
            smac = self.cur_tuner.optimize(
                    name = f"../optimization_results/{self.dbms.name}/smac/", 
                    trials_number=2000,
                    initial_config_number=15) #####原本 为15
            
            self.success_run_last = []
            stage_budget = self.success_per_stage
            while True:
                info = smac.ask()
                st = time.time()
                cost = self.cur_tuner.set_and_replay(config=info.config, seed=info.seed)
                value = TrialValue(cost=cost, time=time.time()-st)
                smac.tell(info, value)
                if cost != 1000000:
                    stage_budget -= 1
                    self.success_run_last.append(str(self.cur_tuner.round))
                if stage_budget == 0:
                    break
            self.round = self.cur_tuner.round
            
            print(f"self.success_run_last:{self.success_run_last}")
            
            time.sleep(1)
            
            self.update_single_dict()
            if self.cur_stage == 1:
                # 第一阶段直接选取子集上表现好的配置尝试
                with open(self.cur_tuner.runhistory_path, 'r') as f:
                    runhistory_data = json.load(f)["data"]
                self.success_run_last = [k for k, v in self.single_dict["data"].items() if 1000000 not in v.values()]
                self.success_run_last = sorted(self.success_run_last, key=lambda k: runhistory_data[int(k)-1][4])
                self.exec_whole_last = self.success_run_last[: int(len(self.success_run_last)*self.verify_ratio)]
                if str(16) not in self.exec_whole_last:
                    self.exec_whole_last.append(str(16))
                self.exec_whole_idx.extend(self.exec_whole_last)

            else:
                self.select_round_to_run()
                
            # 选中的在完整workload执行
            print(f"exec_whole_last:{self.exec_whole_last}")
            self.exec_whole()
                
            # 调优结束
            if time.time() - self.start_time > self.tuning_budget_s:
                print("tuning budget reached")
                print(time.time() - self.start_time)
                return
            
            # 连续几轮没变化就增加压缩率
            self.whole_time_lst = [sum([v1 for k1, v1 in v.items()]) for k, v in self.single_dict["data"].items() if k in self.exec_whole_idx]
            self.cur_incumbent_cost = min(self.whole_time_lst)
            print(f"INCUMBENt:{self.last_incumbent_cost, self.cur_incumbent_cost}")
            if self.cur_incumbent_cost <= self.last_incumbent_cost:
                stage_to_run = self.update_threshold
                self.last_incumbent_cost = self.cur_incumbent_cost
            else:
                stage_to_run -= 1
                
            if stage_to_run == 0:
                stage_to_run = self.update_threshold
                self.comp_ratio += 0.15
                print("increase workload")
            
            
            
            
            self.select_queries()
            self.exec_selected_on_history()
        
