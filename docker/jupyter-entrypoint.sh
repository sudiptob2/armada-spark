#!/bin/bash

cd /home/spark/workspace

exec jupyter notebook \
     --ip=0.0.0.0 \
     --port=8888 \
     --no-browser \
     --allow-root \
     --NotebookApp.token='' \
     --NotebookApp.password='' \
     --NotebookApp.notebook_dir=/home/spark/workspace
