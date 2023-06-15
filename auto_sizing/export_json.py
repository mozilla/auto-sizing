import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

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

    agg_json: dict[str, dict[str, Any]] = {}
    target_results_filename_pattern = rf"[\S*]({ARGO_PREFIX}_\d*).json"
    for blob in storage_client.list_blobs(
        bucket_name, prefix=f"{SAMPLE_SIZE_PATH}/ind_target_results_{today}"
    ):
        # For files in the bucket, check if file name matches `target_\d.json` pattern
        regexp_result = re.search(target_results_filename_pattern, blob.name)
        if regexp_result:
            target_slug = regexp_result.group(1)
            data = blob.download_as_string()

            # parse out recipe fields
            target_recipe = jobs_dict[target_slug]
            recipe = json.loads(target_recipe["target_recipe"])
            app_id = target_recipe.get("app_id")
            channel = recipe.get("release_channel")
            locale = recipe.get("locale")
            country = recipe.get("country")
            new_or_existing = recipe.get("user_type")
            minimum_version = recipe.get("minimum_version")
            recipe_info = {
                "app_id": app_id,
                "channel": channel,
                "locale": locale,
                "country": country,
                "new_or_existing": new_or_existing,
                "minimum_version": minimum_version,
            }
            results = {
                "target_recipe": recipe_info,
                "sample_sizes": json.loads(data),
            }

            # target_key should be an easy lookup for relevant sizing
            # {app_id}:{channel}:{locale}:{country}:{minimum_version}

            target_key = f"{app_id}"
            if channel:
                target_key += f":{channel}"
            if locale:
                eval_locale = eval(locale)
                sorted_locale = sorted(eval_locale) if type(eval_locale) is tuple else [eval_locale]
                # string representation of list includes spaces between elements
                target_key += f":{sorted_locale}".replace(" ", "")
            if country:
                target_key += f":{country}"
            if minimum_version:
                target_key += f":{minimum_version}"

            if target_key not in agg_json:
                agg_json[target_key] = {}
            agg_json[target_key][new_or_existing] = results

            # final structure looks like: (TODO: remove minimum_version)
            """
            {
                "firefox_desktop:release:['EN-CA','EN-US']:US:110": {
                    "new": {
                        "target_recipe": { ... },
                        "sample_sizes": { ... },
                    },
                    "existing": {
                        "target_recipe": { ... },
                        "sample_sizes": { ... },
                    }
                },
                ...
            }
            """

    file_name = f"auto_sizing_results_{today}"
    _upload_str_to_gcs(project_id, bucket_name, file_name, SAMPLE_SIZE_PATH, json.dumps(agg_json))

    file_name_latest = "auto_sizing_results_latest"
    _upload_str_to_gcs(
        project_id, bucket_name, file_name_latest, SAMPLE_SIZE_PATH, json.dumps(agg_json)
    )
