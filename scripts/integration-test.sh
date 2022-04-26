#!/bin/bash

set -e

# TODO: CHANGE PYTEST COMMAND TO BE LESS VERBOSE
# FIX: MAKE PYTEST COMMAND MORE VERBOSE.
poetry run pytest tests/integration -o log_cli=True -rP -vv


# TODO: REMOVE PYTEST COMAMANDS: WORD COUNT
# poetry run tests/integration/test_word_count.py  -o log_cli=True -rP -vv
# poetry run tests/integration/test_word_count.py::test_should_tokenize_words_and_count_them  -o log_cli=True -rP -vv


# TODO: REMOVE PYTEST COMAMANDS: CITY BIKE
# poetry run pytest tests/integration/test_distance_transformer.py  -o log_cli=True -rP -vv
# poetry run pytest tests/integration/test_ingest.py  -o log_cli=True -rP -vv 
# poetry run pytest tests/integration/test_distance_transformer.py::test_should_maintain_all_data_it_reads  -o log_cli=True -rP -vv