import json, time

class HistoryReuser:
    def __init__(self, runner):
        self.runner = runner

    def exec_selected_on_history(self):
        for i in range(1, self.runner.round + 1):
            if str(i) in self.runner.single_dict["data"].keys() and not set(self.runner.cur_workload_queries.keys()).issubset(set(self.runner.single_dict["data"][str(i)].keys())):
                configs = self.runner.single_dict["configs"][str(i)]
                self.runner.dbms.set_config(configs)
                for name, sql in self.runner.cur_workload_queries.items():
                    if name not in self.runner.single_dict["data"][str(i)]:
                        print(f"Executing {name} on config {i}")
                        timeout_seconds = self.runner.timeout - sum([v for k, v in self.runner.single_dict["data"][str(i)].items()])/1e3
                        timeout_seconds = max(timeout_seconds, 0)
                        print(f"Remaining time for current configuration: {timeout_seconds}")
                        t = self.runner.get_sql_time_with_timeout(sql, timeout_seconds)

                        if t == 1000000:
                            del self.runner.single_dict["data"][str(i)]
                            print(f"Configuration {i} is timeout and will be deleted from single.json")
                            break
                        else:
                            self.runner.single_dict["data"][str(i)][name] = t
        self.runner.dump_single_dict()
