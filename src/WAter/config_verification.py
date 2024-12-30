import time, random, json
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor

class ConfigVerifier:
    def __init__(self, runner):
        self.runner = runner
        self.rf = RandomForestRegressor(n_estimators=200)
    
    ########################################################################
    ##### 1. Code related to select promising configurations to verify #####
    ########################################################################

    def select_round_to_run(self):
        self.runner.update_single_dict()
        with open(self.runner.cur_tuner.runhistory_path, 'r') as f:
            runhistory = json.load(f)
        
        epsilon = 0.5
        if random.random() <= epsilon:
            threshold = self.runner.subset_default_score * 1.0 * 1000
            self.rf_update()
            X, _ = self.get_rf_predict_data()
            rf_score = self.rf.predict(X)

            whole_score = {idx:score*(1 - self.runner.comp_ratio) for idx, score in zip(self.runner.success_run_last, rf_score)}
            for i in self.runner.success_run_last:
                whole_score[i] += runhistory["data"][int(i)-1][4] * self.runner.comp_ratio
                if runhistory["data"][int(i)-1][4] > threshold:
                    print(f"Exploitation route eliminate round {i}")
                    del whole_score[i]
            
            print(f"whole_score:{whole_score}")
        else:
            threshold = self.runner.subset_default_score * 1.2 * 1000
            X_known, X_unknown = self.get_raw_X()
            sim_scores = self.set_similarity(X_known, X_unknown)

            self.rf_update()
            X, _ = self.get_rf_predict_data()
            uncertainty_scores = self.uncertainty_scores(X)

            r = len(X_unknown)/(len(X_known) + len(X_unknown))
            whole_score = {idx:r*(1 - sim_score)+(1-r)*uncertainty_score for idx, sim_score, uncertainty_score in zip(self.runner.success_run_last, sim_scores, uncertainty_scores)}
            whole_score = {k:-v for k, v in whole_score.items()}
            for i in self.runner.success_run_last:
                if runhistory["data"][int(i)-1][4] > threshold:
                    print(f"Exploration route eliminate round {i}")
                    del whole_score[i]
            print(f"whole_score:{whole_score}")
        
        self.runner.exec_whole_last = sorted(whole_score, key=whole_score.get)[:min(int(self.runner.success_per_stage * self.runner.verify_ratio), len(whole_score))]
        print(f"exec_whole_last: {self.runner.exec_whole_last}")
        self.runner.exec_whole_idx.extend(self.runner.exec_whole_last)

    def rf_update(self):
        X, Y = self.get_rf_train_data()
        self.rf.fit(X, Y["cost"])

    def get_rf_train_data(self):
        self.runner.update_single_dict()

        X = {knob:[] for knob in self.runner.target_knobs}
        Y = {"cost":[]}

        for i in self.runner.exec_whole_idx:
            for knob in self.runner.target_knobs:
                try:
                    control_para = self.runner.single_dict["configs"][i][f"control_{knob}"]
                    if control_para == "0":
                        value = self.runner.single_dict["configs"][i][knob]       
                    elif control_para == "1":
                        value = self.runner.single_dict["configs"][i][f"special_{knob}"]
                except:
                    value = self.runner.single_dict["configs"][i][knob]
                X[knob].append(value)
            
            Y["cost"].append(sum(list(self.runner.single_dict["data"][str(i)].values())))
        X = pd.DataFrame(X)
        Y = pd.DataFrame(Y)
        self.get_preprocessor()
        X = self.preprocessor.transform(X)
        X = X.astype(np.float32)
        X = np.clip(X, np.finfo(np.float32).min, np.finfo(np.float32).max)
        
        return X, Y

    def get_rf_predict_data(self):
        self.runner.update_single_dict()

        X = {knob:[] for knob in self.runner.target_knobs}
        Y = {"cost":[]}

        for i in self.runner.success_run_last:
            for knob in self.runner.target_knobs:
                try:
                    control_para = self.runner.single_dict["configs"][i][f"control_{knob}"]
                    if control_para == "0":
                        value = self.runner.single_dict["configs"][i][knob] 
                    elif control_para == "1":
                        value = self.runner.single_dict["configs"][i][f"special_{knob}"]
                except:
                    value = self.runner.single_dict["configs"][i][knob]
                X[knob].append(value)

            Y["cost"].append(sum(list(self.runner.single_dict["data"][str(i)].values())))
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
                ('num', StandardScaler(), self.runner.num_knobs),
                ('cat', OneHotEncoder(), self.runner.cat_knobs)
            ])
        self.preprocessor.fit(X)

    def get_raw_X(self):
        self.runner.update_single_dict()

        X_known = {knob:[] for knob in self.runner.target_knobs}
        X_unknown = {knob:[] for knob in self.runner.target_knobs}
        Y = {"cost":[]}

        for i in self.runner.exec_whole_idx:
            for knob in self.runner.target_knobs:
                try:
                    control_para = self.runner.single_dict["configs"][i][f"control_{knob}"]
                    if control_para == "0":
                        value = self.runner.single_dict["configs"][i][knob]       
                    elif control_para == "1":
                        value = self.runner.single_dict["configs"][i][f"special_{knob}"]
                except:
                    value = self.runner.single_dict["configs"][i][knob]
                X_known[knob].append(value)
                
            Y["cost"].append(sum(list(self.runner.single_dict["data"][str(i)].values())))
        X_known = pd.DataFrame(X_known)

        for i in self.runner.success_run_last:
            for knob in self.runner.target_knobs:
                try:
                    control_para = self.runner.single_dict["configs"][i][f"control_{knob}"]
                    if control_para == "0":
                        value = self.runner.single_dict["configs"][i][knob]       
                    elif control_para == "1":
                        value = self.runner.single_dict["configs"][i][f"special_{knob}"]
                except:
                    value = self.runner.single_dict["configs"][i][knob]
                X_unknown[knob].append(value)
                
            Y["cost"].append(sum(list(self.runner.single_dict["data"][str(i)].values())))    
        X_unknown = pd.DataFrame(X_unknown)

        return X_known, X_unknown

    def set_similarity(self, X_known, X_unknown):
        min_values = X_known[self.runner.num_knobs].min()
        max_values = X_known[self.runner.num_knobs].max()
        sims = []
        for i in range(X_unknown.shape[0]):
            biggest_sim = 0.0
            for j in range(X_known.shape[0]):
                sim = self.similarity_func(X_unknown.iloc[i], X_known.iloc[j], min_values, max_values)
                biggest_sim = max(sim, biggest_sim)
            sims.append(biggest_sim)

        return sims
    
    def similarity_func(self, x1, x2, min_values, max_values):
        n_features = len(x1)
        sum_dist = 0.0

        for i in range(n_features):
            if isinstance(x1[i], (int, float)) and isinstance(x2[i], (int, float)):  # Continuous variable
                sum_dist += abs(x1[i] - x2[i]) / (max_values[i] - min_values[i])
            else:  # Categorical variable
                sum_dist += 0 if x1[i] == x2[i] else 1

        return  n_features / (sum_dist + n_features)
    
    def uncertainty_scores(self, X):
        all_predictions = np.zeros((X.shape[0], len(self.rf.estimators_)))
        for i, estimator in enumerate(self.rf.estimators_):
            all_predictions[:, i] = estimator.predict(X)
        uncertainty = np.std(all_predictions, axis=1)
        return uncertainty

    ################################################################################
    ##### 2. Code related to execute whole workload on selected configurations #####
    ################################################################################
    def exec_whole(self):
        whole_to_remove = []

        for i in self.runner.exec_whole_last:
            dbms = self.runner.dbms
            configs = self.runner.single_dict["configs"][str(i)]
            dbms.set_config(configs)
            for name, sql in self.runner.whole_workload_queries.items():
                if name not in self.runner.single_dict["data"][str(i)]:
                    print(f"Test {name} on configuration {i}")
                    timeout_seconds = self.runner.timeout - sum(list(self.runner.single_dict["data"][str(i)].values()))/1e3
                    timeout_seconds = max(0, timeout_seconds)
                    print(f"Remaining time for current configuration: {timeout_seconds}")
                    t = self.runner.get_sql_time_with_timeout(sql, timeout_seconds)
                    if t == 1000000:
                        if str(i) in self.runner.single_dict["data"]:
                            del self.runner.single_dict["data"][str(i)]
                        whole_to_remove.append(i)
                        print(f"Configuration {i} is timeout and will be deleted from single.json")
                        break
                    else:
                        self.runner.single_dict["data"][str(i)][name] = t
            
            if "cost" not in self.runner.single_dict:
                self.runner.single_dict["cost"] = []
            if str(i) in self.runner.single_dict["data"]:
                self.runner.single_dict["cost"].append({"round":str(i), "time": time.time()- self.runner.start_time, "cost":sum(list(self.runner.single_dict["data"][str(i)].values()))})
        
        for i in whole_to_remove:
            self.runner.exec_whole_last.remove(i)
            self.runner.exec_whole_idx.remove(i)
            
        self.runner.dump_single_dict()