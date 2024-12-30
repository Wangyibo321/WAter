import sys, os, json, re, time
from WAter_runner.runner_template import RunnerTemplate
from vanilla_tuner.gptuner.coarse_stage import CoarseStage
from vanilla_tuner.gptuner.fine_stage import FineStage
from smac.runhistory.dataclasses import TrialValue

class RunnerGPTuner(RunnerTemplate):
    def __init__(self, tuner, dbms, timeout, target_knobs_path, seed, whole_workload_queries, workload_name):
        super().__init__(tuner, dbms, timeout, target_knobs_path, seed, whole_workload_queries, workload_name)
        self.record_single_path = self.cur_tuner.record_single_path
        
    def optimize(self):
        self.start_time = time.time()

        ###########################
        ##### 1. Coarse Stage #####
        ###########################
        
        # GPTuner's coarse_stage is recarded as the first time slice 
        self.cur_stage += 1
        self.cur_tuner.workload_queries = self.cur_workload_queries
        print(f"self.cur_tuner.workload_queries：{self.cur_tuner.workload_queries.keys()}")

        # Get subset's performance on default configuration
        self.subset_default_score = 0
        for idx, value in self.time_dict.items():
            if idx in self.cur_tuner.workload_queries:
                self.subset_default_score += value
        print(f"self.subset_default_score：{self.subset_default_score}")
            
        smac = self.cur_tuner.optimize(
            name = f"../optimization_results/{self.dbms.name}/coarse/", 
            trials_number=30, 
            initial_config_number=15)
        smac.optimize()
        self.round = self.cur_tuner.round
        time.sleep(10)
        self.update_single_dict()

        # choose the top-k configurations that perform well on current subset to verify on the whole workload
        with open(self.cur_tuner.runhistory_path, 'r') as f:
            runhistory_data = json.load(f)["data"]
        self.success_run_last = [k for k, v in self.single_dict["data"].items() if self.timeout not in v.values()]
        self.success_run_last = sorted(self.success_run_last, key=lambda k: runhistory_data[int(k)-1][4])
        # configurations perform worse than the default configurations on the current subset are discarded
        self.exec_whole_last = [config for config in self.success_run_last[:min(int(30*self.verify_ratio), len(self.success_run_last))] if runhistory_data[int(config)-1][4] <= self.subset_default_score * 1.0 * 1000]
        if str(16) not in self.exec_whole_last:
            self.exec_whole_last.append(str(16))
        print(f"self.exec_whole_last: {self.exec_whole_last}")
        self.exec_whole_idx.extend(self.exec_whole_last)
        self.verifier.exec_whole()

        #########################
        ##### 2. Fine Stage #####
        #########################
        stage_to_run = self.update_threshold
        self.cur_tuner = self.tuner[1]
        self.cur_tuner.round = self.round

        while True:
            ##############################
            ##### 2.1 Timeout or not #####
            ##############################
            if time.time() - self.start_time > self.tuning_budget_s:
                print("tuning budget reached")
                print(f"total time: {time.time() - self.start_time}")
                return
            self.cur_stage += 1

            ##################################################
            ##### 2.2 Need to increase comp_ratio or not #####
            ##################################################
            self.whole_time_lst = [sum([v1 for k1, v1 in v.items()]) for k, v in self.single_dict["data"].items() if k in self.exec_whole_idx]
            self.cur_incumbent_cost = min(self.whole_time_lst)
            print(f"Incumbent:{self.last_incumbent_cost, self.cur_incumbent_cost}")

            if self.cur_incumbent_cost < self.last_incumbent_cost:
                stage_to_run = self.update_threshold
                self.last_incumbent_cost = self.cur_incumbent_cost
            else:
                stage_to_run -= 1
            
            if stage_to_run == 0:
                stage_to_run = self.update_threshold
                self.comp_ratio += self.comp_ratio_add_unit
                print(f"Increase workload comp_ratio from {self.comp_ratio - self.comp_ratio_add_unit} to {self.comp_ratio}")

            ###############################################################
            ##### 2.3 Obtain a new subset and fill in missing history #####
            ###############################################################
            self.cur_workload_queries = self.compressor.select_queries()
            self.cur_tuner.workload_queries = self.cur_workload_queries
            self.history_reuser.exec_selected_on_history()
            # Get subset's performance on default configuration
            self.subset_default_score = 0
            for idx, value in self.time_dict.items():
                if idx in self.cur_tuner.workload_queries:
                    self.subset_default_score += value
            print(f"self.subset_default_score：{self.subset_default_score}")

            ###################################
            ##### 2.4 Tune the new subset #####
            ###################################
            self.success_run_last = []
            stage_budget = self.success_per_stage
            smac = self.cur_tuner.optimize(
                name = f"../optimization_results/{self.dbms.name}/fine/",
                trials_number=2000) # history trials + new tirals
            while True:
                info = smac.ask()
                st = time.time()
                cost = self.cur_tuner.set_and_replay(config=info.config, seed=info.seed)
                value = TrialValue(cost=cost, time=time.time()-st)
                smac.tell(info, value)
                if cost != self.timeout:
                    stage_budget -= 1
                    self.success_run_last.append(str(self.cur_tuner.round))
                if stage_budget == 0:
                    break
            self.round = self.cur_tuner.round
            print(f"self.success_run_last:{self.success_run_last}")
            time.sleep(1)

            #####################################################################
            ##### 2.5 Verify promising configurations on the whole workload #####
            #####################################################################
            self.verifier.select_round_to_run()
            self.verifier.exec_whole()
            
