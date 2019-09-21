import json
from datetime import datetime
from numpy import array
from graphviz import Digraph

from log_parser.block_manager import BlockManager
from log_parser.executor import Executor
from log_parser.job import Job
from log_parser.job import Task


def get_json(line):
    # Need to first strip the trailing newline, and then escape newlines (which can appear
    # in the middle of some of the JSON) so that JSON library doesn't barf.
    return json.loads(line.strip("\n").replace("\n", "\\n"))


class LogParser:
    def __init__(self, filename, is_logging_enable=False):
        self.filename = filename
        self.parsed_data = {}  # empty dicts.
        self.block_managers = []  # empty lists.

        self.executors = {}
        self.jobs = {}
        self.tasks = {}

        self.is_logging_enable = is_logging_enable

    def do_SparkListenerLogStart(self, data):
        self.parsed_data["spark_version"] = data["Spark Version"]
        # parsed_data is a empty dict.

    def do_SparkListenerBlockManagerAdded(self, data):
        bm = BlockManager(data)
        self.block_managers.append(bm)
        # block_managers is a empty list

    def do_SparkListenerEnvironmentUpdate(self, data):
        self.parsed_data["java_version"] = data["JVM Information"]["Java Version"]
        self.parsed_data["app_name"] = data["Spark Properties"]["spark.app.name"]
        self.parsed_data["app_id"] = data["Spark Properties"]["spark.app.id"]
        # self.parsed_data["driver_memory"] = data["Spark Properties"]["spark.driver.memory"]
        # self.parsed_data["executor_memory"] = data["Spark Properties"]["spark.executor.memory"]
        self.parsed_data["commandline"] = data["System Properties"]["sun.java.command"]

    def do_SparkListenerApplicationStart(self, data):
        self.parsed_data["app_start_timestamp"] = data["Timestamp"]

    def do_SparkListenerApplicationEnd(self, data):
        self.parsed_data["app_end_timestamp"] = data["Timestamp"]

    def do_SparkListenerJobStart(self, data):
        job_id = data["Job ID"]
        if job_id in self.jobs:
            print("ERROR: Duplicate job ID!")
            return
        job = Job(data)  # that class Job
        # job = return s
        self.jobs[job_id] = job  # record into the `dict`

    def do_SparkListenerStageSubmitted(self, data):
        pass

    def do_SparkListenerExecutorAdded(self, data):
        exec_id = data["Executor ID"]
        self.executors[exec_id] = Executor(data)

    def do_SparkListenerTaskStart(self, data):
        task_id = data["Task Info"]["Task ID"]
        self.tasks[task_id] = Task(data)

    def do_SparkListenerTaskEnd(self, data):
        task_id = data["Task Info"]["Task ID"]
        self.tasks[task_id].finish(data)

    def do_SparkListenerExecutorRemoved(self, data):
        exec_id = data["Executor ID"]
        self.executors[exec_id].remove(data)

    def do_SparkListenerBlockManagerRemoved(self, data):
        pass

    def do_SparkListenerStageCompleted(self, data):
        stage_id = data["Stage Info"]["Stage ID"]
        for j in self.jobs.values():
            for s in j.stages:  # class Stage in job.py
                if s.stage_id == stage_id:
                    s.complete(data)

    def do_SparkListenerJobEnd(self, data):
        job_id = data["Job ID"]
        self.jobs[job_id].complete(data)

    def process(self):
        with open(self.filename, "r") as log_file:
            unsupported_event_types = set()

            for line in log_file:
                json_data = get_json(line)
                event_type = json_data["Event"]

                # 13 event types
                if event_type == "SparkListenerLogStart":
                    self.do_SparkListenerLogStart(json_data)
                elif event_type == "SparkListenerBlockManagerAdded":
                    self.do_SparkListenerBlockManagerAdded(json_data)
                elif event_type == "SparkListenerEnvironmentUpdate":
                    self.do_SparkListenerEnvironmentUpdate(json_data)
                elif event_type == "SparkListenerApplicationStart":
                    self.do_SparkListenerApplicationStart(json_data)
                elif event_type == "SparkListenerApplicationEnd":
                    self.do_SparkListenerApplicationEnd(json_data)
                elif event_type == "SparkListenerJobStart":
                    self.do_SparkListenerJobStart(json_data)
                elif event_type == "SparkListenerStageSubmitted":
                    self.do_SparkListenerStageSubmitted(json_data)
                elif event_type == "SparkListenerExecutorAdded":
                    self.do_SparkListenerExecutorAdded(json_data)
                elif event_type == "SparkListenerTaskStart":
                    self.do_SparkListenerTaskStart(json_data)
                elif event_type == "SparkListenerTaskEnd":
                    self.do_SparkListenerTaskEnd(json_data)
                elif event_type == "SparkListenerExecutorRemoved":
                    self.do_SparkListenerExecutorRemoved(json_data)
                elif event_type == "SparkListenerBlockManagerRemoved":
                    self.do_SparkListenerBlockManagerRemoved(json_data)
                elif event_type == "SparkListenerStageCompleted":
                    self.do_SparkListenerStageCompleted(json_data)
                elif event_type == "SparkListenerJobEnd":
                    self.do_SparkListenerJobEnd(json_data)
                else:
                    # print("WARNING: unknown event type: " + event_type)
                    unsupported_event_types.add(event_type)

            if len(unsupported_event_types) > 0 and self.is_logging_enable:
                print("WARNING: unknown event types:\n\t{}".format("\n\t".join(unsupported_event_types)))

        # Link block managers and executors
        for bm in self.block_managers:
            if bm.executor_id != "driver":
                self.executors[bm.executor_id].block_managers.append(bm)

        for t in self.tasks.values():
            self.executors[t.executor_id].tasks.append(t)
            for j in self.jobs.values():
                for s in j.stages:
                    if s.stage_id == t.stage_id:
                        s.tasks.append(t)

        self.parsed_data["num_failed_tasks"] = 0
        self.parsed_data["num_success_tasks"] = 0
        for t in self.tasks.values():
            if t.end_reason != "Success":
                self.parsed_data["num_failed_tasks"] += 1
            else:
                self.parsed_data["num_success_tasks"] += 1

        # Total average and stddev task run time
        all_runtimes = [x.finish_time - x.launch_time for x in self.tasks.values() if x.end_reason == "Success"]
        all_runtimes = array(all_runtimes)
        self.parsed_data["tot_avg_task_runtime"] = all_runtimes.mean()
        self.parsed_data["tot_std_task_runtime"] = all_runtimes.std()
        self.parsed_data["min_task_runtime"] = all_runtimes.min()
        self.parsed_data["max_task_runtime"] = all_runtimes.max()

    def get_app_name(self):
        return self.parsed_data["app_name"]

    def get_app_id(self):
        return self.parsed_data["app_id"]

    def generate_report(self):
        # return s
        s = "Report for '{}' execution {}\n".format(self.parsed_data["app_name"], self.parsed_data["app_id"])
        s += "Spark version: {}\n".format(self.parsed_data["spark_version"])
        s += "Java version: {}\n".format(self.parsed_data["java_version"])
        s += "Application Start time: {}\n".format(
            datetime.fromtimestamp(self.parsed_data["app_start_timestamp"] / 1000))
        s += "Application End time: {}\n".format(datetime.fromtimestamp(self.parsed_data["app_end_timestamp"] / 1000))
        s += "Commandline: {}\n\n".format(self.parsed_data["commandline"])

        s += "---> Jobs <---\n"
        s += "In total, there are {} jobs in {}\n".format(len(self.jobs), self.parsed_data["app_name"])
        s += "\n"
        for j in self.jobs.values():
            s += j.report(0)
            s += "\n"

        s += "---> Tasks <---\n"
        s += "Total tasks: {}\n".format(len(self.tasks))
        s += "Successful tasks: {}\n".format(self.parsed_data["num_success_tasks"])
        s += "Failed tasks: {}\n".format(self.parsed_data["num_failed_tasks"])
        s += "Task average runtime: {} ({} stddev)\n".format(self.parsed_data["tot_avg_task_runtime"],
                                                             self.parsed_data["tot_std_task_runtime"])
        s += "Task min/max runtime: {} min, {} max\n".format(self.parsed_data["min_task_runtime"],
                                                             self.parsed_data["max_task_runtime"])
        for t in self.tasks.values():
            s += t.report(0)
            s += "\n"

        s += "---> Executors <---\n"
        s += "In total, there are {} executors in {}\n".format(len(self.executors), self.parsed_data["app_name"])
        s += "\n"
        for e in self.executors.values():
            s += e.report(0)
            s += "\n"

        s += "---> Block managers <---\n"
        s += "In total, there are {} block managers in {}\n".format(len(self.block_managers),
                                                                    self.parsed_data["app_name"])
        for bm in self.block_managers:
            s += bm.report(0)

        s += "\n"

        # print('generate_report is finished.')

        return s

    def save_plot_of_stages_dag(self, filename, view=False):
        dag = Digraph()

        for j in self.jobs.values():
            for s in j.stages:
                assert type(s.stage_id) == int
                dag.node(str(s.stage_id), str(s.stage_id))

        for j in self.jobs.values():
            for s in j.stages:
                for parent in s.parent_ids:
                    assert type(parent) == int
                    assert type(s.stage_id) == int
                    dag.edge(str(parent), str(s.stage_id))

        dag.render(filename, view=view)
