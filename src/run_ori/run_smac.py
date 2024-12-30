from configparser import ConfigParser
import argparse
import time
import os, json
from smactuner.smactuner import SMACTuner
from dbms.postgres import PgDBMS
from space_optimizer.default_space import DefaultSpace
from smac import HyperparameterOptimizationFacade, Scenario, initial_design, BlackBoxFacade, intensifier
from ConfigSpace import (
    UniformIntegerHyperparameter,
    UniformFloatHyperparameter,
    CategoricalHyperparameter,
    Configuration,
)


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("timeout", type=int)
    parser.add_argument("-seed", type=int, default=1)
    args = parser.parse_args()
    print(f'Input arguments: {args}')
    time.sleep(2)
    config = ConfigParser()
    
    config_path = "./configs/postgres.ini"
    config.read(config_path)
    dbms = PgDBMS.from_file(config)
    
    target_knobs_path = f"./knowledge_collection/postgres/target_knobs.txt"
    with open(target_knobs_path, 'r') as file:
        lines = file.readlines()
        target_knobs = [line.strip() for line in lines]
        
        
    workload_queries=dict()
    workload_path = "/home/a_re_GPTuner_tpcds_dynamic_comp/sql/tpcds_select"
    sqls = os.listdir(workload_path)
    for sql in sqls:
        with open(os.path.join(workload_path, sql), 'r') as file:
            q = file.read()
            workload_queries[sql.split(".")[0]] = q
            
            
    smac_tuner = SMACTuner(
        dbms=dbms, 
        target_knobs_path=target_knobs_path, 
        timeout=args.timeout, 
        seed=args.seed,
        workload_queries = workload_queries
    )
    smac =  smac_tuner.optimize(
                name = f"../optimization_results/{dbms.name}/smac/", 
                trials_number=2000,  ###$$$
                initial_config_number=15) ###$$$
    smac.optimize()
    
# PYTHONPATH=src_new python3 ./src_new/run_ori/run_smac.py  100 -seed=2  | tee log2.txt

    import sys, subprocess
    print("$$FINISH GPTUNER")

    restart_pg_cmd = "su - postgres -c '/usr/lib/postgresql/14/bin/pg_ctl stop -D /var/lib/postgresql/14/main/ -o \"-c config_file=/etc/postgresql/14/main/postgresql.conf\"'"
    reconfig_cmd = "rm /var/lib/postgresql/14/main/postgresql.auto.conf"

    def restart_postgresql():
        try:
            subprocess.run(reconfig_cmd, shell=True, check=True)
        except subprocess.CalledProcessError:
            pass
        subprocess.run(restart_pg_cmd, shell=True, check=True)
        
    restart_postgresql()
    print("STOP")

    sys.exit()
    print("KK")