from datetime import datetime


class Job:
    def __init__(self, start_data):
        # you can find a line with event_type = "SparkListenerJobStart" and copy it into https://jsonformatter.org
        # see the hierarchical structure
        """ example of "SparkListenerJobStart"
        {
          "Event": "SparkListenerJobStart",
          "Job ID": 8,
          "Submission Time": 1499523114857,
          "Stage Infos": [
            {
              "Stage ID": 8,
              "Stage Attempt ID": 0,
              "Stage Name": "collect at KMeans.scala:446",
              "Number of Tasks": 32,
              "RDD Info": [
        """

        self.job_id = start_data["Job ID"]
        self.stages = []

        self.submission_time = start_data["Submission Time"]

        for stage_data in start_data["Stage Infos"]:
            self.stages.append(Stage(stage_data))   # class Stage

        self.result = None
        self.end_time = None

    def complete(self, data):
        """def do_SparkListenerJobEnd(self, data):
            job_id = data["Job ID"]
            self.jobs[job_id].complete(data)

            {
            "Event": "SparkListenerJobEnd",
            "Job ID": 7,
            "Completion Time": 1499523114773,
            "Job Result": {
            "Result": "JobSucceeded"
            }
            }
        """
        self.result = data["Job Result"]["Result"]
        self.end_time = data["Completion Time"]

    def report(self, indent):
        pfx = "\t" * indent
        # indent means 'tab'
        s = pfx + "Job {}\n".format(self.job_id)
        indent += 1
        pfx = "\t" * indent
        s += pfx + "Submission time: {}\n".format(datetime.fromtimestamp(self.submission_time/1000))
        s += pfx + "End time: {}\n".format(datetime.fromtimestamp(self.end_time / 1000))
        s += pfx + "Run time: {}ms \n".format(int(self.end_time or 0) - int(self.submission_time))
        s += pfx + "Result: {}\n".format(self.result)
        s += pfx + "Number of stages: {}\n".format(len(self.stages))
        for stage in self.stages:
            s += stage.report(indent)
            # self.stages.append(Stage(stage_data))
        return s

    def get_runtime(self):
        return int(self.end_time or 0) - int(self.submission_time)


class Stage:
    def __init__(self, stage_data):
        self.stage_id = stage_data["Stage ID"]
        self.name = stage_data["Stage Name"]
        self.parent_ids = stage_data["Parent IDs"] if "Parent IDs" in stage_data else []

        self.details = stage_data["Details"]
        self.task_num = stage_data["Number of Tasks"]
        self.RDDs = []
        self.tasks = []

        for rdd_data in stage_data["RDD Info"]:
            self.RDDs.append(RDD(rdd_data))

        self.completion_time = None
        self.submission_time = None

    def complete(self, data):
        self.completion_time = data["Stage Info"]["Completion Time"]
        self.submission_time = data["Stage Info"]["Submission Time"]

    def report(self, indent):
        pfx = "\t" * indent
        s = pfx + "Stage '{}' (id={})\n".format(self.name, self.stage_id)
        indent += 1
        pfx = "\t" * indent
        s += pfx + "Number of tasks: {}\n".format(self.task_num)
        s += pfx + "Number of executed tasks: {}\n".format(len(self.tasks))
        if len(self.tasks) > 0:
            s += pfx + "Tasks average completion times: {}ms\n".format(self.get_tasks_average_completion_times())
        s += pfx + "Completion time: {}ms\n".format(int(self.completion_time or 0) - int(self.submission_time or 0))
        for rdd in self.RDDs:
            s += rdd.report(indent)
        s += pfx + "Parent IDs: {}\n".format(self.parent_ids)

        return s

    def get_tasks_average_completion_times(self):
        if len(self.tasks) > 0:
            sum_of_task_execution_times = float(0)
            for t in self.tasks:
                sum_of_task_execution_times += int(t.finish_time or 0) - int(t.launch_time or 0)

            return sum_of_task_execution_times / len(self.tasks)
        return 0

    def get_completion_time(self):
        return int(self.completion_time or 0) - int(self.submission_time or 0)

class RDD:
    """
    "RDD Info": [
        {
          "RDD ID": 24,
          "Name": "MapPartitionsRDD",
          "Scope": "{\"id\":\"57\",\"name\":\"mapPartitionsWithIndex\"}",
          "Callsite": "mapPartitionsWithIndex at KMeans.scala:438",
          "Parent IDs": [
            23
          ],
          "Storage Level": {
            "Use Disk": false,
            "Use Memory": false,
            "Deserialized": false,
            "Replication": 1
          },
          "Number of Partitions": 32,
          "Number of Cached Partitions": 0,
          "Memory Size": 0,
          "Disk Size": 0
        }, ...
    """

    def __init__(self, rdd_data):
        self.rdd_id = rdd_data["RDD ID"]
        self.name = rdd_data["Name"]
        self.parent_ids = rdd_data["Parent IDs"] if "Parent IDs" in rdd_data else []

        self.disk_size = rdd_data["Disk Size"]
        self.memory_size = rdd_data["Memory Size"]
        self.partitions = rdd_data["Number of Partitions"]
        self.replication = rdd_data["Storage Level"]["Replication"]

    def report(self, indent):
        pfx = "\t" * indent
        s = pfx + "RDD '{}' (id={})\n".format(self.name, self.rdd_id)
        indent += 1
        pfx = "\t" * indent
        s += pfx + "Size: {}B memory {}B disk\n".format(self.memory_size, self.disk_size)
        s += pfx + "Partitions: {}\n".format(self.partitions)
        s += pfx + "Replication: {}\n".format(self.replication)
        s += pfx + "Parent IDs: {}\n".format(self.parent_ids)
        return s


class Task:
    """
    {
      "Event": "SparkListenerTaskStart",
      "Stage ID": 0,
      "Stage Attempt ID": 0,
      "Task Info": {
        "Task ID": 1,
        "Index": 1,
        "Attempt": 0,
        "Launch Time": 1499523032866,
        "Executor ID": "7",
        "Host": "172.31.47.174",
        "Locality": "PROCESS_LOCAL",
        "Speculative": false,
        "Getting Result Time": 0,
        "Finish Time": 0,
        "Failed": false,
        "Accumulables": []
        }
    }
    """
    def __init__(self, data):
        self.task_id = data["Task Info"]["Task ID"]
        self.stage_id = data["Stage ID"]
        self.executor_id = data["Task Info"]["Executor ID"]

        self.launch_time = data["Task Info"]["Launch Time"]
        self.locality = data["Task Info"]["Locality"]
        self.speculative = data["Task Info"]["Speculative"]

        self.end_reason = None
        self.failed = False
        self.finish_time = None
        # def calc_task_times(self):
        self.getting_result_time = None
        self.index = None
        self.type = None

        self.has_metrics = False
        self.disk_spilled_bytes = None
        self.memory_spilled_bytes = None
        self.executor_deserialize_time = None
        self.executor_run_time = None
        self.jvm_gc_time = None
        self.result_serialize_time = None
        self.result_size = None

    def finish(self, data):
        """
        {
          "Event": "SparkListenerTaskEnd",
          "Stage ID": 0,
          "Stage Attempt ID": 0,
          "Task Type": "ResultTask",
          "Task End Reason": {
            "Reason": "Success"
          },
          "Task Info": {
            "Task ID": 0,
            "Index": 0,
            "Attempt": 0,
            "Launch Time": 1499523032831,
            "Executor ID": "6",
            "Host": "172.31.47.174",
            "Locality": "PROCESS_LOCAL",
            "Speculative": false,
            "Getting Result Time": 0,
            "Finish Time": 1499523035339,
            "Failed": false,
            "Accumulables": [
              {
                "ID": 0,
                "Name": "internal.metrics.executorDeserializeTime",
                "Update": 1945,
                "Value": 1945,
                "Internal": true,
                "Count Failed Values": true
              },...
        """
        self.end_reason = data["Task End Reason"]["Reason"]
        self.failed = data["Task Info"]["Failed"]
        self.finish_time = data["Task Info"]["Finish Time"]
        self.getting_result_time = data["Task Info"]["Getting Result Time"]
        self.index = data["Task Info"]["Index"]
        self.type = data["Task Type"]  # "Task Type": "ResultTask"

        if "Task Metrics" in data:
            self.has_metrics = True
            self.disk_spilled_bytes = data["Task Metrics"]["Disk Bytes Spilled"]
            self.memory_spilled_bytes = data["Task Metrics"]["Memory Bytes Spilled"]
            self.executor_deserialize_time = data["Task Metrics"]["Executor Deserialize Time"]
            self.executor_run_time = data["Task Metrics"]["Executor Run Time"]
            self.jvm_gc_time = data["Task Metrics"]["JVM GC Time"]
            self.result_serialize_time = data["Task Metrics"]["Result Serialization Time"]
            self.result_size = data["Task Metrics"]["Result Size"]

    def report(self, indent):
        pfx = "\t" * indent
        s = pfx + "Task {} (stage: {}, executor: {})\n".format(self.task_id, self.stage_id, self.executor_id)
        indent += 1
        pfx = "\t" * indent
        s += pfx + "Started at: {}\n".format(datetime.fromtimestamp(self.launch_time / 1000))
        s += pfx + "Ended at: {}\n".format(datetime.fromtimestamp(self.finish_time / 1000) if self.finish_time else None)
        s += pfx + "Run time: {}ms\n".format(int(self.finish_time or 0) - int(self.launch_time or 0))
        assert self.finish_time is not None
        assert self.launch_time is not None
        s += pfx + "End reason: {}\n".format(self.end_reason)
        s += pfx + "Locality: {}\n".format(self.locality)
        s += pfx + "Speculative: {}\n".format(self.speculative)
        s += pfx + "Type: {}\n".format(self.type)
        s += pfx + "Index: {}\n".format(self.index)
        if self.has_metrics:
            s += pfx + "Metrics:\n"
            indent += 1
            pfx = "\t" * indent
            s += pfx + "Spilled bytes: {}B memory, {}B disk\n".format(self.memory_spilled_bytes, self.disk_spilled_bytes)
            s += pfx + "Executor deserialize time: {}ms\n".format(self.executor_deserialize_time)
            s += pfx + "Executor run time: {}ms\n".format(self.executor_run_time)
            s += pfx + "JVM GC time: {}ms\n".format(self.jvm_gc_time)
            s += pfx + "Result serialize time: {}ms\n".format(self.result_serialize_time)
            s += pfx + "Result size: {}B\n".format(self.result_size)
        return s
