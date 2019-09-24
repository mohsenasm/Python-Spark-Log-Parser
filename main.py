import os
import sys
import glob
import re
from collections import defaultdict
from slugify import slugify

from analysis.plot import plot_all_stages
from log_parser.parser import LogParser


def parse_application_log(file_path, all_apps):
    log_parser = LogParser(file_path)
    try:
        log_parser.process()
    except KeyError as e:
        print(f"error on parse {file_path}, {e}")
        return
    name = log_parser.get_app_name()
    id = log_parser.get_app_id()
    safe_name = slugify(name+"_"+id)
    if len(safe_name) == 0:
        safe_name = file_path.split("/")[-1]

    # report = log_parser.generate_report()
    # with open(os.path.expanduser(f"output/report_{safe_name}"), "w") as report_file:
    #     report_file.write(report)
    # log_parser.save_plot_of_stages_dag(f"output/dag_{safe_name}")
    #
    # print(f"Log processing of application '{safe_name}' completed.")

    regex = r"query([0-9]*)_cluster_([0-9]*)G"
    matches = re.finditer(regex, name)

    for match in matches:
        groups = list(match.groups())

        query_number, data_scale =  int(groups[0]), int(groups[1])
        all_apps[query_number][data_scale] = log_parser

        break

def parse_application_log_from_directory(directory):
    files = glob.glob(directory + "/*")

    apps = defaultdict(dict)

    for file in files:
        parse_application_log(file, apps)

    return apps

if __name__ == "__main__":
    if len(sys.argv) < 1:
        print("Usage: python3 <main.py> <log_dir>")
    else:
        apps = parse_application_log_from_directory(os.path.join(sys.argv[1]))
        plot_all_stages(apps[26], "query_26")
        plot_all_stages(apps[52], "query_52")

