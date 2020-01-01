FROM python:3.7

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    graphviz && rm -rf /var/lib/apt/lists/*

COPY . /log-parser
RUN pip3 install -r /log-parser/requirements.txt

WORKDIR /files
ENTRYPOINT ["python3", "main.py"]
