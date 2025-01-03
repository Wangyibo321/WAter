# WAter: A Workload-Adaptive Knob Tuning System

- This repository hosts the source code and supplementary materials for our VLDB 2025 submission, WAter: A Workload-Adaptive Knob Tuning System
- WAter is an adaptive knob tuning system that uses runtime profile to significantly reduce benchmark evaluation costs by only selecting SQL subsets to evaluate at different time slices.

## Table of Contents

- [System Overview](#system-overview)
- [Quick Start](#quick-start)
- [Experimental Results](#experimental-result)
- [Code Structure](#code-structure)

## System Overview

<img src="/assets/workflow.png" alt="WAter workflow" width="1000">

**WAter** is a runtime-efficient workload adaptive knob tuning system that divides the tuning process into many time slices, and evaluate only a small subset of queries from the workload within each slice, instead of replaying the whole workload repeatedly. The figure above presents the tuning workflow, the whole tuning process is made of a sequence of time slices and each time slice involves three steps:

:one: Given an input workload, WAter greedily compresses the workload based on runtime statistics to maximize _representativity_. Since there is no runtime profile at the beginning, WAter uses GSUM or random sampling to initialize the subset.

:two: WAter organizes and reuses the tuning history when tuning other subsets, to bootstrap the local surrogate model of the tuner assigned for the current sub set to tune, achieving efficient subset tuning. 

:three: WAter uses heuristic-based rules to prune unpromising configurations, then ranks and chooses a proportion (e.g., 30%) of configurations proposed in step <span style="background-color:black; color:white; border-radius:50%; padding:0px 0px; font-size:16px; display:inline-block; text-align:center; width:16px; height:16px;line-height:16px;">2</span> of the current time slice to be verified on the entire workload. In the first time slice, the configurations are ranked based on their performances on the subset.

## Quick Start

The following instructions have been tested on Ubuntu 22.04 and PostgreSQL v14.9:

### Step 1: Install PostgreSQL

```
sudo apt-get update
sudo apt-get install postgresql-14
```

### Step 2: Install Benchmarks

- Please refer to the corresponding repositories for the following benchmarks:
  - [TPC-H](https://github.com/cmu-db/benchbase)
  - [TPC-DS](https://github.com/gregrahn/tpcds-kit)
  - [JOB](https://github.com/RyanMarcus/imdb_pg_dataset)

### Step 3: Install dependencies

```
sudo pip install -r requirements.txt
```

### Step 4: Execute WAter to optimize your DBMS

- Note: modify `configs/postgres.ini` to determine the target DBMS first, the `restart` and `recover` commands depend on the environment and we provide Docker version.
- Note: modify `configs/water_params.ini` to determine WAter's hyper-parameters.
- Run WAter

```
# PYTHONPATH=src python3 src/run/WAter/run_smac.py <seed> | tee <log>
PYTHONPATH=src python3 src/run/WAter/run_smac.py -seed=100 | tee log100.txt

# PYTHONPATH=src python3 src/run/WAter/run_gptuner.py <seed> | tee <log>
PYTHONPATH=src python3 src/run/WAter/run_gptuner.py -seed=200 | tee log200.txt
```

- We also provide vanilla SMAC and GPTuner:

```
# PYTHONPATH=src python3 src/run/vanilla_tuner/run_smac.py <seed> | tee <log>
PYTHONPATH=src python3 src/run/vanilla_tuner/run_smac.py -seed=300 | tee log300.txt

# PYTHONPATH=src python3 src/run/vanilla_tuner/run_gptuner.py <seed> | tee <log>
PYTHONPATH=src python3 src/run/vanilla_tuner/run_gptuner.py -seed=400 | tee log400.txt
```

### Step 6: View the optimization result:

The optimization result is stored in `optimization_results/{dbms}/history_{seed}.json`, where  `{seed}` is the random seed given by user.

- the `data` block contains the execution time of executed queries on the configuration proposed in the corresponding tuning iteration.
- the `"configs"` block contains the knob configuration of the i-th iteration, for example:

```
"configs": {
    "1": {
      "effective_io_concurrency": 200,
      "random_page_cost": 1.2 
    },
}
```


## Experimental Result

### Baselines

We integrate WAter with SMAC and GPTuner, and compare WAter with state-of-the-art methods:

- SMAC: the best Bayesian Optimiztion (BO)-based method evaluated in an [Experimental Evaluation VLDB'22](https://dl.acm.org/doi/10.14778/3538598.3538604)
- GPTuner:  the best knowledge-enhanced method that uses GPT to read the manual to guide the Bayesian Optimization. [VLDB'24](https://vldb.org/pvldb/vol17/p1939-tang.pdf)
- GSUM: a recent generic workload compression system that maximizes the coverage of features (e.g., columns contained) of the workload while ensuring that the compressed workload remains representative (i.e., having similar distribution to that of the entire workload). [VLDB'20](http://vldb.org/pvldb/vol14/p418-deep.pdf)
- Random: we uniformly picks random samples from the workload.

### Result on PostgreSQL

We compare WAter with baselines on PostgreSQL, and use TPC-DS, JOB, TPC-H and TPC-H$\times$10 as the target workloads. The metric is the execution time of the workloads. For more details and more experiments, please refer to our paper.

<img src="/assets/result.png" alt="result" width="1000">

## Code Structure

- `configs/`
	- `postgres.ini`: Configuration file to optimize PostgreSQL
	- `water_params.ini`: Configuration file to determine WAter's hyper-parameters
- `gsum_init_sql/`
	- `job/`
	  - `job_0.2.json`: Initial subset given by GSUM with `comp_ratio=0.2`
	  - `job_0.3.json`: Initial subset given by GSUM with `comp_ratio=0.3`
	
	- `tpcds_select/`
	  - `tpcds_select_0.2.json`: Initial subset given by GSUM with `comp_ratio=0.2`
	  - `tpcds_select_0.3.json`: Initial subset given by GSUM with `comp_ratio=0.3`
	
	- `tpch/`
	  - `tpch_0.2.json`: Initial subset given by GSUM with `comp_ratio=0.2`
	  - `tpch_0.3.json`: Initial subset given by GSUM with `comp_ratio=0.3`
	
	- `tpchx10/`
	  - `tpchx10_0.2.json`: Initial subset given by GSUM with `comp_ratio=0.2`
	  - `tpchx10_0.3.json`: Initial subset given by GSUM with `comp_ratio=0.3`
	


- `knowledge_collection/`
  - `postgres/`
    - `target_knobs.txt`: List of target knobs for PostgreSQL tuning
    - `knob_info/`
      - `system_view.json`: Information from PostgreSQL system views (pg_settings)
      - `official_document.json`: Information from PostgreSQL official documentation
    - `knowledge_sources/`
      - `gpt/`: Knowledge sourced from GPT models
      - `manual/`: Knowledge from DBMS manuals
      - `web/`: Knowledge extracted from web sources
      - `dba/`: Knowledge from database administrators
    - `tuning_lake/`: Data lake for DBMS tuning knowledge
    - `structured_knowledge/`
      - `special/`: Specialized structured knowledge
      - `normal/`: General structured knowledge
- `optimization_results/`
	- `postgres/`
	    - `coarse/`: Coarse-stage optimization results for PostgreSQL
	    - `fine/`: Fine-stage optimization results for PostgreSQL
	    - `log/`: Optimization logs for PostgreSQL
	    - `single/`: WAter optimization results for PostgreSQL
	    - `smac/`: SMAC optimization results for PostgreSQL
- `scripts/`
	- `recover_postgres.sh`: Script to recover the state of PostgreSQL database
- `src/`
	- `dbms/`
	    - `dbms_template.py`: Template for database management systems
	    - `postgres.py`: Implementation for PostgreSQL
	- `run/`
	    - `vanilla_tuner/`
	        - `run_gptuner.py`: Run vanilla GPTuner
	        - `run_smac.py`: Run vanilla SMAC
	    - `WAter/`
	        - `run_gptuner.py`: Run GPTuner-based WAter
	        - `run_smac.py`: Run SMAC-based WAter
	- `space_optimizer/`
	    - `gptuner_space/`
	        - `knob_selection.py`: Module for knob selection
	        - `coarse_space.py`: Definition of coarse search space
	        - `fine_space.py`: Definition of fine search space
	    - `default_space.py`: Definition of default search space
	- `vanilla_tuner/`
	    - `gptuner/`
	        - `coarse_stage.py`: Recommender for coarse stage configuration
	        - `fine_stage.py`: Recommender for fine stage configuration
	    - `smactuner/`
	        - `smactuner.py`: Recommender for SMAC
	- `WAter/`
	    - `workload_compression.py`: Module for workload compression (**Sec. 5**)
	    - `history_reuse.py`: Module for history reuse mechanism in subset tuning (**Sec. 6**)
	    - `config_verification.py`: Module for verify promising configurations (**Sec. 7**)
	- `WAter_runner/`
	    - `runner_template.py`: Template for integrating a knob tuning method with WAter
	    - `runner_gptuner.py`: Implementation for “WAter + GPTuner”
	    - `runner_smac.py`: Implementation for “WAter + SMAC”
- `workload/`
    - `job/`: SQL statements from JOB
    - `tpcds_select/`: SQL statements from TPC-DS (selected)
    - `tpch/`: 1 SQL statements for each of the 22 templates from TPC-H
    - `tpchx10/`:  10 randomly generated SQL statements for each of the 22 templates from TPC-H
