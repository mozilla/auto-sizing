import json
import logging
import re
from pathlib import Path
from typing import Any

import google.cloud.storage as storage
import toml
from mozilla_nimbus_schemas.jetstream import SampleSizes, SizingRecipe

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


def parse_recipe_from_slug(target_slug: str) -> SizingRecipe:
    jobs_dict = toml.load(RUN_MANIFEST)
    print(jobs_dict)
    # assert False

    # parse out recipe fields
    target_recipe = jobs_dict[target_slug]
    recipe = json.loads(target_recipe["target_recipe"])
    app_id = target_recipe.get("app_id")
    channel = recipe.get("release_channel")
    locale = recipe.get("locale")
    country = recipe.get("country")
    new_or_existing = recipe.get("user_type")
    recipe_info = {
        "app_id": app_id,
        "channel": channel,
        "locale": locale,
        "country": country,
        "new_or_existing": new_or_existing,
    }

    return recipe_info


def build_target_key_from_recipe(recipe_info: SizingRecipe) -> str:
    # parse out recipe fields
    app_id = recipe_info.get("app_id")
    channel = recipe_info.get("channel")
    locale = recipe_info.get("locale")
    country = recipe_info.get("country")

    # target_key should be an easy lookup for relevant sizing
    # {app_id}:{channel}:{locale}:{country}

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

    return target_key


def aggregate_results(
    project_id: str,
    bucket_name: str,
    today,
) -> SampleSizes:
    storage_client = storage.Client(project_id)

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

            recipe_info = parse_recipe_from_slug(target_slug)

            results = {
                "target_recipe": recipe_info,
                "sample_sizes": json.loads(data),
            }

            target_key = build_target_key_from_recipe(recipe_info)
            new_or_existing = recipe_info.get("new_or_existing")

            if target_key not in agg_json:
                agg_json[target_key] = {}
            agg_json[target_key][new_or_existing] = results

    # validate json before export
    sizing_results = SampleSizes.parse_obj(agg_json) if agg_json is not None else {}

    return sizing_results


def upload_aggregate_json(
    project_id: str,
    bucket_name: str,
    results: SampleSizes,
    today: str,
):
    file_name = f"auto_sizing_results_{today}"
    sizing_json = results.json()
    _upload_str_to_gcs(project_id, bucket_name, file_name, SAMPLE_SIZE_PATH, sizing_json)

    file_name_latest = "auto_sizing_results_latest"
    _upload_str_to_gcs(
        project_id,
        bucket_name,
        file_name_latest,
        SAMPLE_SIZE_PATH,
        sizing_json,
    )


def aggregate_and_reupload(
    project_id: str,
    bucket_name: str,
    run_date: str,
) -> None:
    sizing_results = aggregate_results(project_id, bucket_name, run_date)

    upload_aggregate_json(project_id, bucket_name, sizing_results, run_date)
