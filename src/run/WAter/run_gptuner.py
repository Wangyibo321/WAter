import argparse, time, os, sys, json
from configparser import ConfigParser
from dbms.postgres import PgDBMS
from vanilla_tuner.gptuner.coarse_stage import CoarseStage
from vanilla_tuner.gptuner.fine_stage import FineStage
from WAter_runner.runner_gptuner import RunnerGPTuner

if __name__ == '__main__':
    def get_time_dict(dbms, time_dict_path, whole_workload_queries):
        dbms.reset_config()
        dbms.reconfigure()
        time_dict = dict()
        for name, sql in whole_workload_queries.items():
            start_time = time.time()
            dbms.exec_queries(sql)
            t = time.time() - start_time
            time_dict[name] = t
        with open(time_dict_path, 'w') as f:
            json.dump(time_dict, f, indent=4)
        return time_dict

    parser = argparse.ArgumentParser()
    parser.add_argument("-seed", type=int, default=1)
    args = parser.parse_args()
    print(f'Input arguments: {args}')
    time.sleep(2)
    config = ConfigParser()

    config_path = "./configs/postgres.ini"
    config.read(config_path)
    dbms = PgDBMS.from_file(config)

    target_knobs_path = "./knowledge_collection/postgres/target_knobs.txt"

    # read the target workload
    workload_name = 'tpcds_select'
    workload_queries = dict()
    workload_path = os.path.join('workload', workload_name)
    sqls = os.listdir(workload_path)
    for sql in sqls:
        with open(os.path.join(workload_path, sql), 'r') as f:
            q = f.read()
        workload_queries[sql.split(".")[0]] = q

    # get timeout (2 * (workload exectution time on default configuration))
    time_dict_path = "./time_dict.json"
    try:
        with open(time_dict_path, 'r') as f:
            time_dict = json.load(f)
    except:
        print("Executing workload on default config to get 'time_dict.json'.")
        time_dict = get_time_dict(dbms, time_dict_path, workload_queries)
        print("get_time_dict() finishes.")
    timeout = 2 * sum(time_dict.values())
    
    tuner = []
    tuner.append(CoarseStage(
        dbms=dbms, 
        target_knobs_path=target_knobs_path, 
        timeout=timeout, 
        seed=args.seed,
        workload_queries = workload_queries
    ))
    tuner.append(FineStage(
        dbms=dbms, 
        target_knobs_path=target_knobs_path, 
        timeout=timeout, 
        seed=args.seed,
        workload_queries = workload_queries
    ))

    # construct "WAter + GPTuner" runner
    runner_gptuner = RunnerGPTuner(tuner, dbms, timeout, target_knobs_path, args.seed, workload_queries, workload_name)
    runner_gptuner.optimize()
    
# PYTHONPATH=src python3 src/run/WAter/run_gptuner.py -seed=600  | tee log600.txt