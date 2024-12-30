from abc import ABC, abstractmethod
from space_optimizer.default_space import DefaultSpace
from dbms.postgres import PgDBMS
from space_optimizer.fine_space import FineSpace
import json
from smac import HyperparameterOptimizationFacade, Scenario, initial_design, intensifier
from ConfigSpace import (
    UniformIntegerHyperparameter,
    UniformFloatHyperparameter,
    CategoricalHyperparameter,
    Configuration,
)

class FineStage(FineSpace):

    def __init__(self, dbms, timeout, target_knobs_path, seed, workload_queries):
        super().__init__(dbms, timeout, target_knobs_path, seed, workload_queries)
        self.runhistory_path = f"./optimization_results/{self.dbms.name}/fine/{self.seed}/runhistory.json"

    def optimize(self, name, trials_number):
        scenario = Scenario(
            configspace=self.search_space,
            name = name,
            deterministic=True,
            n_trials=trials_number,
            seed=self.seed,
        )
        init_design = initial_design.DefaultInitialDesign(
            scenario,
        )
        
        try:
            with open(self.fine_path, 'r') as f:
                his_configs = json.load(f)["configs"]
        except:
            with open(self.coarse_path, 'r') as f:
                his_configs = json.load(f)["configs"]

        query_names = self.workload_queries.keys()
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

        configs, config_costs = [], []
        for index, value_cost in enumerate(costs):
        # for index, value in index_min_pairs:
            config_id = index + 1
            config_value_dict = his_configs[str(config_id)]
            # make type transformation from coarse to fine 
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
            
        
        smac = HyperparameterOptimizationFacade(
            scenario=scenario,
            initial_design=init_design,
            target_function=self.set_and_replay,
            intensifier=intensifier.Intensifier(scenario, retries=trials_number),
            # acquisition_maximizer=optimizer,
            overwrite=True,
        )
        for config, config_cost in zip(configs, config_costs):
            smac.runhistory.add(config, config_cost, seed=self.seed)
        # smac.optimize()
        return smac
