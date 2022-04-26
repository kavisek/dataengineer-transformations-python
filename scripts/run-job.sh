#!/bin/bash

set -e

poetry build


# TODO: COMMENT THE CURRENT INPUT_FILE WHEN RUNNING JOB
INPUT_FILE_PATH="./resources/citibike/citibike.csv"
# INPUT_FILE_PATH="./resources/word_count/words.txt"
OUTPUT_PATH="./output"

rm -rf $OUTPUT_PATH

poetry run spark-submit \
    --master local \
    --py-files dist/data_transformations-*.whl \
    $JOB \
    $INPUT_FILE_PATH \
    $OUTPUT_PATH