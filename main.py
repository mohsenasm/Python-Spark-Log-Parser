import os
import sys
import glob
from slugify import slugify

from log_parser.parser import LogParser


def parse_application_log(file_path):
    log_parser = LogParser(file_path)
    log_parser.process()
    name = log_parser.get_app_name()
    id = log_parser.get_app_id()
    safe_name = slugify(name+"_"+id)
    if len(safe_name) == 0:
        safe_name = file_path.split("/")[-1]

    report = log_parser.generate_report()
    with open(os.path.expanduser(f"output/report_{safe_name}"), "w") as report_file:
        report_file.write(report)
    log_parser.save_plot_of_stages_dag(f"output/dag_{safe_name}")

    print(f"Log processing of application '{safe_name}' completed.")

if __name__ == "__main__":
    if len(sys.argv) < 1:
        print("Usage: python3 <main.py> <log_dir>")
    else:
        path = os.path.join(sys.argv[1])
        files = glob.glob(path + "/*")
        for file in files:
            parse_application_log(file)
