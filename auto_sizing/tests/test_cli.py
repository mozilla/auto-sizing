from auto_sizing.cli import run
import warnings

warnings.filterwarnings("ignore")

string = """
--target_slug test_local_save 
--project-id moz-fx-data-bq-data-science 
--dataset_id mbowerman 
--local_config /Users/mbowerman/repos/auto-sizing/auto_sizing/tests/data/test_config.toml"
"""
command = string.split(" ")
run(command)


string = """
--bucket moz-data-science-mbowerman-bucket 
--target_slug test_gcs_bucket_save 
--project-id moz-fx-data-bq-data-science 
--dataset_id mbowerman 
--local_config /Users/mbowerman/repos/auto-sizing/auto_sizing/tests/data/test_config.toml"
"""
command = string.split(" ")
run(command)
