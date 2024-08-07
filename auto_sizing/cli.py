import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, List, Mapping, Optional, Protocol, TextIO, Type

import attr
import click
import pytz
import toml
from jetstream.argo import submit_workflow
from jetstream.logging import LOG_SOURCE

from .errors import NoConfigFileException
from .export_json import aggregate_and_reupload
from .logging import LogConfiguration
from .size_calculation import SizeCalculation
from .targets import SizingCollection, SizingConfiguration
from .utils import dict_combinations

logger = logging.getLogger(__name__)


@attr.s
class AllType:
    """Sentinel value for AnalysisExecutor"""


All = AllType()

DATA_DIR = Path(__file__).parent / "data"
RUN_MANIFEST = DATA_DIR / "manifest.toml"
TARGET_SETTINGS = DATA_DIR / "target_lists.toml"


class ExecutorStrategy(Protocol):
    project_id: str

    def __init__(self, project_id: str, dataset_id: str, *args, **kwargs) -> None: ...

    def execute(
        self,
        worklist: Iterable[SizingConfiguration],
        configuration_map: Optional[Mapping[str, TextIO]] = None,
    ) -> bool: ...


@attr.s(auto_attribs=True)
class ArgoExecutorStrategy:
    project_id: str
    dataset_id: str
    bucket: str
    zone: str
    cluster_id: str
    monitor_status: bool
    cluster_ip: Optional[str] = None
    cluster_cert: Optional[str] = None
    experiment_getter: Callable = SizingCollection.from_repo

    WORKFLOW_DIR = Path(__file__).parent / "workflows"
    RUN_WORKFLOW = WORKFLOW_DIR / "run.yaml"

    def execute(
        self,
        worklist: Iterable[SizingConfiguration],
    ):
        targets_list = [{"slug": config.target_slug} for config in worklist]
        logger.debug(f"TARGETS LIST: {targets_list}")

        return submit_workflow(
            project_id=self.project_id,
            zone=self.zone,
            cluster_id=self.cluster_id,
            workflow_file=self.RUN_WORKFLOW,
            parameters={
                "targets": targets_list,
                "project_id": self.project_id,
                "dataset_id": self.dataset_id,
                "bucket": self.bucket,
            },
            monitor_status=self.monitor_status,
            cluster_ip=self.cluster_ip,
            cluster_cert=self.cluster_cert,
        )


@attr.s(auto_attribs=True)
class SerialExecutorStrategy:
    project_id: str
    dataset_id: str
    bucket: str
    sizing_class: Type = SizeCalculation
    experiment_getter: Callable = SizingCollection.from_repo

    def execute(self, worklist: List[SizingConfiguration]):
        failed = False
        for config in worklist:
            try:
                sizing = self.sizing_class(self.project_id, self.dataset_id, self.bucket, config)
                sizing.run(datetime.now(tz=pytz.utc).date())

            except Exception as e:
                logger.exception(str(e), exc_info=e, extra={"target": config.target_slug})
                failed = True

        return not failed


@attr.s(auto_attribs=True)
class AnalysisExecutor:
    project_id: str
    dataset_id: str
    bucket: str
    configuration_file: Optional[TextIO] = attr.ib(None)
    target_slug: str = attr.ib(None)
    run_preset_jobs: Optional[bool] = False
    refresh_manifest: Optional[bool] = False

    @staticmethod
    def _today() -> datetime:
        return datetime.combine(
            datetime.now(tz=pytz.utc).date() - timedelta(days=1),
            datetime.min.time(),
            tzinfo=pytz.utc,
        )

    def execute(
        self,
        strategy: ExecutorStrategy,
        *,
        today: Optional[datetime] = None,
    ) -> bool:
        target_collection = SizingCollection()

        worklist = self._target_list_to_analyze(target_collection)

        return strategy.execute(worklist)

    def _target_list_to_analyze(
        self, target_collection: SizingCollection
    ) -> List[SizingConfiguration]:
        if self.configuration_file:
            sizing_job = target_collection.from_file(self.configuration_file)
            return self._target_to_sizingconfigurations_file(sizing_job)

        elif self.run_preset_jobs:
            jobs_dict = toml.load(TARGET_SETTINGS)
            jobs_manifest = toml.load(RUN_MANIFEST)
            if isinstance(self.target_slug, AllType):
                if self.refresh_manifest:
                    target_list = dict_combinations(jobs_dict, "targets")
                    jobs_manifest = {}

                    target_num = 0
                    for app_id in ["firefox_desktop", "firefox_ios", "fenix"]:
                        for target in target_list:
                            jobs_manifest[f"argo_target_{target_num}"] = {
                                "app_id": app_id,
                                "target_recipe": json.dumps(target),
                            }
                            target_num += 1
                    with open(RUN_MANIFEST, "w") as f:
                        toml.dump(jobs_manifest, f)
                worklist = []
                for target_slug, job_target in jobs_manifest.items():
                    sizing_collections = target_collection.from_repo(
                        json.loads(job_target["target_recipe"]),
                        jobs_dict,
                        app_id=job_target["app_id"],
                    )
                    sizing_config = self._target_to_sizingconfigurations_repo(
                        sizing_collections, target_slug
                    )
                    worklist.extend(sizing_config)
                return worklist

            else:
                job_target = jobs_manifest[self.target_slug]
                sizing_collections = target_collection.from_repo(
                    json.loads(job_target["target_recipe"]),
                    jobs_dict,
                    app_id=job_target["app_id"],
                )

                return self._target_to_sizingconfigurations_repo(sizing_collections)

        else:
            raise NoConfigFileException

    def _target_to_sizingconfigurations_file(
        self,
        target_list: SizingCollection,
    ) -> List[SizingConfiguration]:
        config = SizingConfiguration(
            target_list.sizing_targets,
            target_slug=self.target_slug if self.target_slug else "",
            metric_list=target_list.sizing_metrics,
            start_date=target_list.sizing_dates["start_date"],
            num_dates_enrollment=target_list.sizing_dates["num_dates_enrollment"],
            analysis_length=target_list.sizing_dates["analysis_length"],
            parameters=target_list.sizing_parameters,
            config_file=self.configuration_file,
        )

        return [config]

    def _target_to_sizingconfigurations_repo(
        self, target: SizingCollection, target_slug: str = ""
    ) -> List[SizingConfiguration]:
        config = SizingConfiguration(
            target.sizing_targets,
            target_slug=target_slug if target_slug else self.target_slug,
            metric_list=target.sizing_metrics,
            start_date=target.sizing_dates["start_date"],
            num_dates_enrollment=target.sizing_dates["num_dates_enrollment"],
            analysis_length=target.sizing_dates["analysis_length"],
            parameters=target.sizing_parameters,
        )

        return [config]


log_project_id_option = click.option(
    "--log_project_id",
    "--log-project-id",
    default="moz-fx-data-experiments",
    help="GCP project to write logs to",
)
log_dataset_id_option = click.option(
    "--log_dataset_id",
    "--log-dataset-id",
    default="monitoring",
    help="Dataset to write logs to",
)
log_table_id_option = click.option(
    "--log_table_id", "--log-table-id", default="logs", help="Table to write logs to"
)
log_source = click.option(
    "--log-source",
    "--log_source",
    default=LOG_SOURCE.SIZING,
    type=LOG_SOURCE,
    help="Source column for logs",
)


@click.group()
@log_project_id_option
@log_dataset_id_option
@log_table_id_option
@click.option("--log_to_bigquery", "--log-to-bigquery", is_flag=True, default=False)
@log_source
@click.pass_context
def cli(
    ctx,
    log_project_id,
    log_dataset_id,
    log_table_id,
    log_to_bigquery,
    log_source,
):
    log_config = LogConfiguration(
        log_project_id,
        log_dataset_id,
        log_table_id,
        log_to_bigquery,
        log_source=log_source,
    )
    log_config.setup_logger()
    ctx.ensure_object(dict)
    ctx.obj["log_config"] = log_config


class ClickDate(click.ParamType):
    name = "run-date"

    def convert(self, value, param, ctx):
        if isinstance(value, datetime):
            return value
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=pytz.utc)


project_id_option = click.option(
    "--project_id",
    "--project-id",
    default="moz-fx-data-experiments",
    help="Project to write to",
)
dataset_id_option = click.option(
    "--dataset_id", "--dataset-id", help="Dataset to write to", required=True
)
target_slug_option = click.option(
    "--target_slug",
    "--target-slug",
    help="Slug for sizing job that is applied to saved files and tables",
    required=False,
)
config_file_option = click.option(
    "--local_config",
    "config_file",
    "--local-config",
    "--config-file",
    help="Path to local config TOML file that contains settings for sizing job",
    type=click.File("rt"),
)
bucket_option = click.option("--bucket", help="GCS bucket to write to", required=False)
run_presets_option = click.option(
    "--run_presets",
    "--run-presets",
    help="Run auto sizing jobs defined in the manifest TOML",
    is_flag=True,
    default=False,
)
refresh_manifest_option = click.option(
    "--refresh_manifest",
    "--refresh-manifest",
    help="Refresh the auto sizing jobs manifest based on the targets list TOML",
    is_flag=True,
    default=False,
)

zone_option = click.option(
    "--zone", default="us-central1-a", help="Kubernetes cluster zone", required=True
)
cluster_id_option = click.option(
    "--cluster_id",
    "--cluster-id",
    default="jetstream",
    help="Kubernetes cluster name",
    required=True,
)

monitor_status_option = click.option(
    "--monitor_status",
    "--monitor-status",
    default=True,
    help="Monitor the status of the Argo workflow",
)

cluster_ip_option = click.option(
    "--cluster_ip",
    "--cluster-ip",
    help="Kubernetes cluster IP address",
)

cluster_cert_option = click.option(
    "--cluster_cert",
    "--cluster-cert",
    help="Kubernetes cluster certificate used for authenticating to the cluster",
)
run_date_option = click.option(
    "--run-date",
    type=ClickDate(),
    help="Run date for which to aggregate and export existing results.",
    metavar="YYYY-MM-DD",
    required=False,
)


@cli.command()
@target_slug_option
@project_id_option
@dataset_id_option
@bucket_option
@config_file_option
@run_presets_option
@click.pass_context
def run(
    ctx,
    target_slug,
    project_id,
    dataset_id,
    bucket,
    config_file,
    run_presets,
):
    """Runs analysis for the provided date."""
    if not run_presets and not config_file:
        raise Exception("Either provide a config file or run auto sizing presets.")

    analysis_executor = AnalysisExecutor(
        target_slug=target_slug,
        project_id=project_id,
        dataset_id=dataset_id,
        bucket=bucket,
        configuration_file=config_file if config_file else None,
        run_preset_jobs=run_presets,
    )

    success = analysis_executor.execute(
        strategy=SerialExecutorStrategy(project_id, dataset_id, bucket),
    )

    sys.exit(0 if success else 1)


@cli.command()
@project_id_option
@dataset_id_option
@target_slug_option
@bucket_option
@zone_option
@cluster_id_option
@monitor_status_option
@cluster_ip_option
@cluster_cert_option
@refresh_manifest_option
def run_argo(
    project_id,
    dataset_id,
    target_slug,
    bucket,
    zone,
    cluster_id,
    monitor_status,
    cluster_ip,
    cluster_cert,
    refresh_manifest,
):
    """Runs analysis for the provided date using Argo."""
    if not bucket:
        raise Exception("A GCS bucket must be provided to save results from runs using Argo.")

    strategy = ArgoExecutorStrategy(
        project_id=project_id,
        dataset_id=dataset_id,
        bucket=bucket,
        zone=zone,
        cluster_id=cluster_id,
        monitor_status=monitor_status,
        cluster_ip=cluster_ip,
        cluster_cert=cluster_cert,
    )

    AnalysisExecutor(
        project_id=project_id,
        dataset_id=dataset_id,
        bucket=bucket,
        target_slug=target_slug if target_slug else All,
        run_preset_jobs=True,
        refresh_manifest=refresh_manifest,
    ).execute(strategy=strategy)


@cli.command()
@project_id_option
@bucket_option
@run_date_option
def export_aggregate_results(project_id, bucket, run_date):
    """
    Retrieves all results from an auto_sizing Argo run from a GCS bucket.
    Aggregates those results into one JSON file and reuploads to that bucket.
    """
    if bucket is None:
        raise ValueError("A GCS bucket must be provided to export aggregate results.")

    run_date_str = datetime.today().strftime("%Y-%m-%d")
    if run_date is None:
        logger.info(f"No date specified, using today's date ({run_date_str})")
    else:
        run_date_str = run_date.strftime("%Y-%m-%d")

    aggregate_and_reupload(project_id=project_id, bucket_name=bucket, run_date=run_date_str)


def refresh_manifest_file(target_lists_file=TARGET_SETTINGS, manifest_file=RUN_MANIFEST):
    jobs_dict = toml.load(target_lists_file)
    target_list = dict_combinations(jobs_dict, "targets")
    jobs_manifest = {}

    target_num = 0
    for app_id in ["firefox_desktop", "firefox_ios", "fenix"]:
        for target in target_list:
            jobs_manifest[f"argo_target_{target_num}"] = {
                "app_id": app_id,
                "target_recipe": json.dumps(target),
            }
            target_num += 1
    with open(manifest_file, "w") as f:
        logger.info(f"Exporting manifest to {manifest_file}")
        toml.dump(jobs_manifest, f)


@cli.command()
@click.option(
    "--target-lists-file",
    "--target-lists",
    default=TARGET_SETTINGS,
    help="Path to TOML file that contains target lists from which to generate a manifest TOML.",
    type=click.File("rt"),
)
@click.option(
    "--manifest-file",
    "--manifest",
    default=RUN_MANIFEST,
    help="Path to TOML file where refreshed manifest should be written.",
    type=click.File("wt"),
)
def refresh_manifest(target_lists_file, manifest_file):
    """
    Retrieves the target_lists.toml file and generates a new manifest.toml.
    """

    refresh_manifest_file()
