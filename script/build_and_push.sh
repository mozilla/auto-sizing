#!/bin/sh

pip install --require-virtualenv . && \
docker build . -t gcr.io/moz-fx-data-experiments/auto_sizing:latest && \
docker push gcr.io/moz-fx-data-experiments/auto_sizing:latest && \
echo
echo "Done."
echo

