import re
import google.cloud.storage as storage
import logging


logger = logging.getLogger(__name__)
SAMPLE_SIZE_PATH = "sample_sizes"


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
    target_file = f"{base_name}_{bq_normalize_name(target_slug)}"
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
) -> None:
    """Export sample sizes to GCS."""
    # TODO: Add logging for sizing results

    _upload_str_to_gcs(project_id, bucket_name, target_slug, SAMPLE_SIZE_PATH, sample_size_result)
