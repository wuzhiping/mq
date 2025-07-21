FROM python:3.12-slim-bookworm

RUN apt-get update && \
    apt-get install -y redis-server vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ADD . /src
WORKDIR /src

RUN pip install jupyterlab

RUN pip install fastapi redis gunicorn uvicorn

CMD /bin/bash -c  "redis-server /src/redis.conf --daemonize yes && jupyter lab --allow-root --ip=0.0.0.0 --NotebookApp.token='12345678' & cd MQ ;gunicorn -w 5 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000 --timeout 120 --log-level debug --access-logfile -  MQ:app "
