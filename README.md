# Experiment Population Auto-Sizing Tool

This tool automatically calculates sizing for experiments.


## Local Installation
```
# Create and activate a python virtual environment.
python3 -m venv venv/
source venv/bin/activate
pip install -r requirements.txt

# Install auto_sizing
pip install .
```

## Production Build
Docker images are built automatically via CI (see `.circleci/config.yml`) whenever a PR is merged to `main`.

## Deployment
Deployment is done similarly to [Jetstream](github.com/mozilla/jetstream): Docker container is built and pushed to GCR, where Argo manages orchestration of tasks in GKE. This tool leverages the existing Jetstream Argo cluster.
