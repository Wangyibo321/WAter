from configparser import ConfigParser
import argparse
import time
import os, json
from dbms.postgres import PgDBMS
from space_optimizer.default_space import DefaultSpace
from smac import HyperparameterOptimizationFacade, Scenario, initial_design, BlackBoxFacade, intensifier
from ConfigSpace import (
    UniformIntegerHyperparameter,
    UniformFloatHyperparameter,
    CategoricalHyperparameter,
    Configuration,
)



class SMACTuner(DefaultSpace):

    def __init__(self, dbms, timeout, target_knobs_path, seed, workload_queries):
        super().__init__(dbms, timeout, target_knobs_path, seed, workload_queries)
        self.define_search_space()
        self.runhistory_path = f"./optimization_results/{self.dbms.name}/smac/{self.seed}/runhistory.json"


    def define_search_space(self):
        """ Define the search space of the optimization algorithm, return 'ConfigSpace' object from SMAC3 """
        knob_info = self.dbms.knob_info
        for knob_name in self.target_knobs.copy():
            info = knob_info[knob_name]
            if info is None: # there is no knob_info for this target_knob, just ignore it
                self.target_knobs.remove(knob_name)
                continue
            
            knob = self.get_default_space(knob_name, info)
            self.search_space.add_hyperparameter(knob)

    
    def optimize(self, name, trials_number, initial_config_number):
        run_flag = True
        scenario = Scenario(
            configspace=self.search_space,
            name = name,
            deterministic=True,
            n_trials=trials_number,
            use_default_config=True,
            seed=self.seed,
        )
        init_design = initial_design.LatinHypercubeInitialDesign(
            scenario,
            n_configs=initial_config_number,
        )
        query_names = self.workload_queries.keys()
        try: #是否已经有历史记录
            with open(self.record_single_path, "r") as json_file:
                data = json.load(json_file)
            costs = []
            for i in range(1, self.round+1):
                cost = 0
                if str(i) in data["data"].keys():
                    for name in query_names:
                        c = data["data"][str(i)][name]
                        cost += c
                else:
                    print(f"illegal config round:{i} not in data")
                    cost=1000000
                costs.append(cost)
                
            with open(self.runhistory_path, "r") as json_file:
                his_configs = json.load(json_file)["configs"]

            configs, config_costs = [], []
            for index, value_cost in enumerate(costs):
                config_id = index + 1
                config_value_dict = his_configs[str(config_id)] 
                transfer_config_value_dict = {}
                for key, value in config_value_dict.items():
                    if key.startswith("control_") or key.startswith("special_"):
                        transfer_config_value_dict[key] = value
                        continue
                    hp = self.search_space[key]
                    if isinstance(hp, CategoricalHyperparameter):
                        transfer_config_value_dict[key] = str(value)
                    elif isinstance(hp, UniformIntegerHyperparameter):
                        transfer_config_value_dict[key] = int(value) 
                    elif isinstance(hp, UniformFloatHyperparameter):
                        transfer_config_value_dict[key] = float(value)
                    else:
                        transfer_config_value_dict[key] = value
                config = Configuration(self.search_space, transfer_config_value_dict)
                configs.append(config)
                config_costs.append(value_cost)
        except Exception as e:
            print(f"Error in reading history data:{e}")
            run_flag = False
            pass

        smac = HyperparameterOptimizationFacade(
            scenario=scenario,
            initial_design=init_design,
            target_function=self.set_and_replay,
            intensifier=intensifier.Intensifier(scenario, retries=trials_number),
            overwrite=True,
        )
        if run_flag:
            for config, config_cost in zip(configs, config_costs):
                smac.runhistory.add(config, config_cost, seed=self.seed)
        
        return smac