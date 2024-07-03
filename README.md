# Experiment Population Auto-Sizing Tool

This tool automatically calculates sizing for experiments.

Currently, pre-computed size calculation supports targeting clients on locale, country, channel, or user type, which includes new or existing clients. New clients are defined
as clients whose first seen date is during the 7 day enrollment period of the size calculation analysis; existing clients are defined as clients whose first seen date was at least
28 days before the first date of enrollment in the size calculation analysis. For any target dimension, the string `'all'` can be included to omit that condition from client selection.


## Development Info
### Local Installation
```
# Create and activate a python virtual environment.
python3 -m venv venv/
source venv/bin/activate
pip install -r requirements.txt

# Install auto_sizing
pip install .
```

### Updating Pre-computed Targets or Parameters
`auto_sizing/data/target_lists.toml` contains the list of targets and configuration parameters (including metrics). Update this file to change the set of targets to pre-compute.

*Note* that `locale` is an array of stringified tuples, each of which is a discrete set of locale combinations. In other words, if you include `EN-US` and `EN-UK` in the list separately, there would be no pre-computed sizing for the combination of `EN-US, EN-UK`. To include combinations, add `"('EN-US', 'EN-UK')"` as its own entry in the list.

#### Refresh Manifest
The file `auto_sizing/data/manifest.toml` contains the target recipes and must be generated from the `target_lists.toml`. Run `auto_sizing refresh-manifest` to refresh the local file, or add the `--refresh-manifest` flag to CLI execution for `run-argo`.

### Production Build
Docker images are built automatically via CI (see `.circleci/config.yml`) whenever a PR is merged to `main`.

### Deployment
Deployment is done similarly to [Jetstream](github.com/mozilla/jetstream): Docker container is built and pushed to GCR, where Argo manages orchestration of tasks in GKE. This tool leverages the existing Jetstream Argo cluster.
