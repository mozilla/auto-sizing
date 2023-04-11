import json
import logging
import re
from datetime import datetime
from pathlib import Path

import google.cloud.storage as storage
import toml

logger = logging.getLogger(__name__)
SAMPLE_SIZE_PATH = "sample_sizes"
DATA_DIR = Path(__file__).parent / "data"
RUN_MANIFEST = DATA_DIR / "manifest.toml"
ARGO_PREFIX = "argo_target"


def bq_normalize_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def _upload_str_to_gcs(
    project_id: str,
    bucket_name: str,
    target_slug: str,
    base_name: str,
    str_to_upload: str,
) -> None:
    storage_client = storage.Client(project_id)
    bucket = storage_client.get_bucket(bucket_name)
    target_file_prefix = base_name.split("/")[0]
    target_file = f"{target_file_prefix}_{bq_normalize_name(target_slug)}"
    target_path = base_name
    blob = bucket.blob(f"{target_path}/{target_file}.json")

    logger.info(f"Uploading {target_file} to {bucket_name}/{target_path}")

    blob.upload_from_string(
        data=str_to_upload,
        content_type="application/json",
    )


def export_sample_size_json(
    project_id: str,
    bucket_name: str,
    target_slug: str,
    sample_size_result: str,
    current_date: str,
) -> None:
    """Export sample sizes to GCS bucket."""

    if ARGO_PREFIX in target_slug:
        _upload_str_to_gcs(
            project_id,
            bucket_name,
            target_slug,
            f"{SAMPLE_SIZE_PATH}/ind_target_results_{current_date}",
            sample_size_result,
        )
    else:
        _upload_str_to_gcs(
            project_id,
            bucket_name,
            target_slug,
            SAMPLE_SIZE_PATH,
            sample_size_result,
        )


def aggregate_and_reupload(
    project_id: str,
    bucket_name: str,
) -> None:
    today = datetime.today().strftime("%Y-%m-%d")
    storage_client = storage.Client(project_id)
    jobs_dict = toml.load(RUN_MANIFEST)

    agg_json = {}
    target_results_filename_pattern = rf"[\S*]({ARGO_PREFIX}_\d*).json"
    for blob in storage_client.list_blobs(
        bucket_name, prefix=f"{SAMPLE_SIZE_PATH}/ind_target_results_{today}"
    ):
        # For files in the bucket, check if file name matches `target_\d.json` pattern
        regexp_result = re.search(target_results_filename_pattern, blob.name)
        if regexp_result:
            target_slug = regexp_result.group(1)
            data = blob.download_as_string()
            results = {
                "target_recipe": jobs_dict[target_slug],
                "sample_sizes": json.loads(data),
            }
            agg_json[target_slug] = results

    file_name = f"auto_sizing_results_{today}"
    _upload_str_to_gcs(project_id, bucket_name, file_name, SAMPLE_SIZE_PATH, json.dumps(agg_json))
