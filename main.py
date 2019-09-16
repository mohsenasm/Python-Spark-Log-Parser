import os
import sys

from log_parser.parser import LogParser


def parse_application_log(path, name=None):
    log_parser = LogParser(path)
    log_parser.process()

    report = log_parser.generate_report()
    with open(os.path.expanduser("output/report.txt"), "w") as report_file:
        report_file.write(report)
    log_parser.save_plot_of_stages_dag("output/dag", view=True)

    print(f"Log processing of application '{log_parser.get_app_name()}' completed.")

if __name__ == "__main__":
    if len(sys.argv) < 1:
        print("Usage: python3 <main.py> <log_file>")
    else:
        path = os.path.join(sys.argv[1])
        parse_application_log(path)
