from auto_sizing.cli import run
from pathlib import Path

local_config = str(Path(__file__).parent / "data/test_config.toml")
dataset_id = "mbowerman"
project_id = "moz-fx-data-bq-data-science"
string = f"--target_slug test_local_save --project-id {project_id} --dataset_id {dataset_id} --local_config {local_config}"
command = string.split(" ")
# run(command)
# runs `auto_sizing run --target_slug test_local_save --project-id moz-fx-data-bq-data-science --dataset_id mbowerman --local_config /Users/mbowerman/repos/auto-sizing/auto_sizing/tests/data/test_config.toml`


from auto_sizing.cli import AnalysisExecutor
from auto_sizing.targets import SizingCollection

test_executor = AnalysisExecutor(
    project_id="", dataset_id="", bucket="", target_slug="", run_preset_jobs=True
)
_ = test_executor._target_list_to_analyze(SizingCollection())
