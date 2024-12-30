import argparse
import time
import os
import sys
from configparser import ConfigParser
from dbms.postgres import PgDBMS
from smactuner.smactuner import SMACTuner
from ablation.smac_const import RunnerSMAC


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

    tuner = []
    tuner.append(SMACTuner(
        dbms=dbms, 
        target_knobs_path=target_knobs_path, 
        timeout=args.timeout, 
        seed=args.seed,
        workload_queries = workload_queries
    ))


    runner_smac = RunnerSMAC(tuner, dbms, args.timeout, target_knobs_path, args.seed, workload_queries)
    
    runner_smac.optimize()

    # PYTHONPATH=src python3 ./src/run_comp/run_smac.py  100 -seed=3  | tee log2.txt