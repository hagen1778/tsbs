#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-vmetrics-data.gz}
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-20s}
DATABASE_NAME=${DATABASE_NAME:-"benchmark"}
source ${EXE_DIR}/load_common.sh


# Load new data
cat ${DATA_FILE} | gunzip | ./tsbs_load_vmetrics \
                                --db-name=${DATABASE_NAME} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${PROGRESS_INTERVAL} \
