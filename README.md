# Spark-Log-Parser

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

## Usage

```
python3 main.py <log_dir>
```
Then reports and stages DAGs will be stored in the `output` directory.

## Use with Docker

First go to the parent directory of `spark-history-directory`, Then:

```
alias spark-parser='docker run -ti --rm -v `pwd`:/files mohsenasm/python-spark-log-parser'
spark-parser spark-history-directory
```

Then, for fixing the permission issue, use this command:
    sudo chown -R $USER:$USER parser_output/

## Reference
- https://github.com/kayousterhout/trace-analysis
- https://github.com/DistributedSystemsGroup/SparkEvents
- based on https://github.com/xiandong79/Spark-Log-Parser
