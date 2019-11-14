# Spark-Log-Parser

## Disclaimer

This is a work-in-progress project. (WIP)

## Setup

For Debian based OS (like Ubuntu):
```
sudo apt-get install graphviz
pip3 install -r requirements.txt
```

For Mac:
```
brew install graphviz
```

## Run

```
python3 main.py <log_dir>
```
Then reports and stages DAGs will be stored in the `output` directory.

## Reference
- https://github.com/kayousterhout/trace-analysis
- https://github.com/DistributedSystemsGroup/SparkEvents
- based on https://github.com/xiandong79/Spark-Log-Parser
