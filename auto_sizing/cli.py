import attr
from datetime import datetime, timedelta
from typing import Optional, Type, Callable, Iterable, Mapping, TextIO, Protocol, Union
import logging

logger = logging.getLogger(__name__)

from mozanalysis.utils import hash_ish
from .size_calculation import SizeCalculation
from .logging import LogConfiguration
from .targets import SizingCollection, SizingConfiguration, MetricsLists
from .errors import NoConfigFileException
import pytz
import click
import sys


@attr.s
class AllType:
    """Sentinel value for AnalysisExecutor"""


All = AllType()


class ExecutorStrategy(Protocol):
    project_id: str

    def __init__(self, project_id: str, dataset_id: str, *args, **kwargs) -> None:
        ...

    def execute(
        self,
        worklist: Iterable[SizingConfiguration],
        configuration_map: Optional[Mapping[str, TextIO]] = None,
    ) -> bool:
        ...


@attr.s(auto_attribs=True)
class SerialExecutorStrategy:
    project_id: str
    dataset_id: str
    bucket: str
    sizing_class: Type = SizeCalculation
    experiment_getter: Callable[[], SizingCollection] = SizingCollection.from_repo

    def execute(
        self,
        worklist: Iterable[SizingConfiguration],
    ):
        failed = False
        for config in worklist:
            try:
                sizing = self.sizing_class(self.project_id, self.dataset_id, self.bucket, config)
                sizing.run(datetime.now(tz=pytz.utc).date())

            except Exception as e:
                failed = True

        return not failed


@attr.s(auto_attribs=True)
class AnalysisExecutor:
    project_id: str
    dataset_id: str
    bucket: str
    target_slug: Optional[str]
    configuration_file: Optional[TextIO] = attr.ib(None)
    app_id: str = "firefox_desktop"
    run_preset_jobs: bool = False

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

        configs = self._target_list_to_analyze(target_collection)

        return strategy.execute(configs)

    def _target_list_to_analyze(
        self, target_collection: SizingCollection
    ) -> Iterable[SizingCollection]:
        if self.configuration_file:
            target_list = target_collection.from_file(self.configuration_file)
            return self._target_list_to_sizingconfigurations_file(target_list)
        elif self.run_preset_jobs:
            target_list = target_collection.from_repo(app_id=self.app_id)
            return self._target_list_to_sizingconfigurations_repo(target_list)
        else:
            raise NoConfigFileException

    def _target_list_to_sizingconfigurations_file(
        self,
        target_list: Iterable[SizingCollection],
    ) -> Iterable[SizingConfiguration]:

        configs = [
            SizingConfiguration(
                target_list.sizing_targets,
                target_slug=self.target_slug if self.target_slug else "",
                metric_list=target_list.sizing_metrics,
                start_date=target_list.sizing_dates["start_date"],
                num_dates_enrollment=target_list.sizing_dates["num_dates_enrollment"],
                analysis_length=target_list.sizing_dates["analysis_length"],
                parameters=target_list.sizing_parameters,
                config_file=self.configuration_file,
            )
        ]

        return configs

    def _target_list_to_sizingconfigurations_repo(
        self,
        target_list: Iterable[SizingCollection],
    ) -> Iterable[SizingConfiguration]:
        configs = []

        for target in target_list.sizing_targets:
            config = SizingConfiguration(
                target,
                target_slug=hash_ish(target.select_expr),
                metric_list=target_list.sizing_metrics,
                start_date=target_list.sizing_dates["start_date"],
                num_dates_enrollment=target_list.sizing_dates["num_dates_enrollment"],
                analysis_length=target_list.sizing_dates["analysis_length"],
                parameters=target_list.sizing_parameters,
            )
            configs.append(config)

        return configs


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


@click.group()
@log_project_id_option
@log_dataset_id_option
@log_table_id_option
@click.option("--log_to_bigquery", "--log-to-bigquery", is_flag=True, default=False)
@click.pass_context
def cli(
    ctx,
    log_project_id,
    log_dataset_id,
    log_table_id,
    log_to_bigquery,
):
    log_config = LogConfiguration(
        log_project_id,
        log_dataset_id,
        log_table_id,
        log_to_bigquery,
    )
    log_config.setup_logger()
    ctx.ensure_object(dict)
    ctx.obj["log_config"] = log_config


class ClickDate(click.ParamType):
    name = "date"

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
sizing_name_option = click.option(
    "--target_slug", help="Name applied to files and tables", required=True
)
config_file_option = click.option("--local_config", "config_file", type=click.File("rt"))
bucket_option = click.option("--bucket", help="GCS bucket to write to", required=False)
app_id_option = click.option("--app_id", "--app_name", help="Firefox app name")
run_presets_option = click.option("--run_presets", hidden=True, is_flag=True, default=False)


@cli.command()
@sizing_name_option
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
    analysis_executor = AnalysisExecutor(
        target_slug=target_slug,
        project_id=project_id,
        dataset_id=dataset_id,
        bucket=bucket,
        configuration_file=config_file if config_file else None,
        run_preset_jobs=run_presets
    )

    success = analysis_executor.execute(
        strategy=SerialExecutorStrategy(project_id, dataset_id, bucket),
    )

    sys.exit(0 if success else 1)
