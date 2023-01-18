from auto_sizing.cli import run
from pathlib import Path

local_config = str(Path(__file__).parent / "data/test_config.toml")
string = "--target_slug test_local_save --project-id moz-fx-data-bq-data-science --dataset_id mbowerman --local_config /Users/mbowerman/repos/auto-sizing/auto_sizing/tests/data/test_config.toml"
command = string.split(" ")
run(command)
# runs `auto_sizing run --target_slug test_local_save --project-id moz-fx-data-bq-data-science --dataset_id mbowerman --local_config /Users/mbowerman/repos/auto-sizing/auto_sizing/tests/data/test_config.toml`
