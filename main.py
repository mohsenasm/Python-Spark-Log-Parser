import os
import sys

from log_parser.parser import LogParser

if __name__ == "__main__":
    if len(sys.argv) < 1:
        print("Usage: python3 <main.py> <log_file>")
    else:
        path = os.path.join(sys.argv[1])
        log_parser = LogParser(path)
        log_parser.process()

        report = log_parser.generate_report()
        open(os.path.expanduser("report.txt"), "w").write(report)

        print(f"Log processing of application '{log_parser.get_app_name()}' completed.")
