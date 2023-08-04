from datetime import datetime
from typing import Dict, List, Literal, Optional, TextIO

import attr
import toml
from mozanalysis.config import ConfigLoader
from mozanalysis.metrics import DataSource, Metric
from mozanalysis.segments import Segment, SegmentDataSource
from mozanalysis.utils import add_days

from .errors import MetricsTagNotFoundException, SegmentsTagNotFoundException
from .utils import default_dates_dict, dict_combinations

ALLOWED_APPS = Literal["firefox_desktop", "firefox_ios", "fenix"]


class SegmentsList:
    """Builds list of Segments from list of dictionaries"""

    def from_repo(
        self, target_list: Dict, app_id: ALLOWED_APPS, start_date: str = ""
    ) -> List[Segment]:
        if app_id == "firefox_desktop":
            return self._make_desktop_targets(target_list, start_date)
        elif app_id == "firefox_ios":
            return self._make_ios_targets(target_list, start_date)
        elif app_id == "fenix":
            return self._make_fenix_targets(target_list, start_date)
        else:
            raise ValueError(
                "Invalid app_id: must be in ('firefox_desktop', 'firefox_ios', 'fenix')"
            )

    def from_file(self, target_dict: Dict, path: TextIO) -> List[Segment]:
        if "segments" not in target_dict.keys():
            raise SegmentsTagNotFoundException(path)

        segments_dict = target_dict["segments"]
        if "import_from_metric_hub" in segments_dict.keys():
            for app_id, segments in segments_dict["import_from_metric_hub"].items():
                for segment in segments:
                    segments_dict[segment] = ConfigLoader.get_segment(segment, app_id)
            segments_dict.pop("import_from_metric_hub")

        if (
            "data_sources" in segments_dict.keys()
            and "import_from_metric_hub" in segments_dict["data_sources"].keys()
        ):
            for app_id, segment_data_sources in segments_dict["data_sources"][
                "import_from_metric_hub"
            ].items():
                for segment_data_source in segment_data_sources:
                    segments_dict["data_sources"][
                        segment_data_source
                    ] = ConfigLoader.get_segment_data_source(segment_data_source, app_id)
            segments_dict["data_sources"].pop("import_from_metric_hub")

        Segment_list = []
        for key, value in segments_dict.items():
            if key == "data_sources":
                continue
            if isinstance(value, Segment):
                Segment_list.append(value)
            else:
                data_source = segments_dict["data_sources"][value["data_source"]]
                Segment_list.append(
                    Segment(
                        name=key,
                        data_source=SegmentDataSource(
                            name="", from_expr=data_source["from_expression"]
                        ),
                        select_expr=ConfigLoader.configs.get_env()
                        .from_string(value["select_expression"])
                        .render(),
                    )
                )

        return Segment_list

    def _make_desktop_targets(self, target: Dict[str, str], start_date: str = "") -> List[Segment]:
        clients_daily = ConfigLoader.get_segment_data_source("clients_daily", "firefox_desktop")

        clients_daily_sql = self._desktop_sql(target)
        Segment_list = []
        Segment_list.append(
            Segment(
                name="clients_daily_filter",
                data_source=clients_daily,
                select_expr=clients_daily_sql,
            )
        )

        clients_last_seen = SegmentDataSource(
            name="clients_last_seen",
            from_expr="`moz-fx-data-shared-prod.telemetry.clients_last_seen`",
        )
        if target["user_type"] == "new":
            Segment_list.append(
                Segment(
                    name="clients_last_seen_filter",
                    data_source=clients_last_seen,
                    select_expr=f"COALESCE(MIN(first_seen_date)  >= '{start_date}', TRUE)",
                )
            )
        elif target["user_type"] == "existing":
            Segment_list.append(
                Segment(
                    name="clients_last_seen_filter",
                    data_source=clients_last_seen,
                    select_expr="""COALESCE(MIN(first_seen_date) <= '{first_day}', TRUE)
                    AND COALESCE(MIN(days_since_seen) = 0)""".format(
                        first_day=add_days(start_date, -28)
                    ),
                )
            )

        return Segment_list

    def _desktop_sql(self, target: Dict[str, str]) -> str:
        clients_daily_sql = """
        COALESCE(LOGICAL_OR(
        (normalized_channel = '{channel}') AND
        (UPPER(locale) in {locale}) AND
        (country = '{country}')
        )
        )
        """.format(
            channel=target["release_channel"],
            locale=target["locale"],
            country=target["country"],
        )

        return clients_daily_sql

    def _make_ios_targets(self, target: Dict[str, str], start_date: str) -> List[Segment]:
        clients_daily = SegmentDataSource(
            name="clients_daily", from_expr="mozdata.org_mozilla_ios_firefox.baseline_clients_daily"
        )
        clients_daily_sql = self._ios_sql(target)

        Segment_list = []
        Segment_list.append(
            Segment(
                name="clients_daily_filter",
                data_source=clients_daily,
                select_expr=clients_daily_sql,
            )
        )

        if target["user_type"] == "new":
            baseline_clients_first_seen = SegmentDataSource(
                name="baseline_clients_first_seen",
                from_expr="`moz-fx-data-shared-prod.org_mozilla_ios_firefox.baseline_clients_first_seen`",  # noqa: E501
            )
            Segment_list.append(
                Segment(
                    name="clients_last_seen_filter",
                    data_source=baseline_clients_first_seen,
                    select_expr=f"COALESCE(MIN(first_seen_date)  >= '{start_date}', TRUE)",
                )
            )
        elif target["user_type"] == "existing":
            baseline_clients_last_seen = SegmentDataSource(
                name="baseline_clients_last_seen",
                from_expr="`moz-fx-data-shared-prod.org_mozilla_ios_firefox.baseline_clients_last_seen`",  # noqa: E501
            )
            Segment_list.append(
                Segment(
                    name="clients_last_seen_filter",
                    data_source=baseline_clients_last_seen,
                    select_expr="""COALESCE(MIN(first_seen_date) <= '{first_day}', TRUE)
                    AND COALESCE(MIN(days_since_seen) = 0)""".format(
                        first_day=add_days(start_date, -28)
                    ),
                )
            )

        return Segment_list

    def _ios_sql(self, target: Dict) -> str:
        clients_daily_sql = """
        COALESCE(LOGICAL_OR(
        (normalized_channel = '{channel}') AND
        (UPPER(locale) in {locale}) AND
        (country = '{country}')
        )
        )
        """.format(
            channel=target["release_channel"],
            locale=target["locale"],
            country=target["country"],
        )

        return clients_daily_sql

    def _make_fenix_targets(self, target: Dict[str, str], start_date: str) -> List[Segment]:
        clients_daily = SegmentDataSource(
            name="clients_daily", from_expr="mozdata.org_mozilla_firefox.baseline_clients_daily"
        )

        clients_daily_sql = self._fenix_sql(target)
        Segment_list = []
        Segment_list.append(
            Segment(
                name="clients_daily_filter",
                data_source=clients_daily,
                select_expr=clients_daily_sql,
            )
        )
        if target["user_type"] == "new":
            baseline_clients_first_seen = SegmentDataSource(
                name="baseline_clients_first_seen",
                from_expr="`moz-fx-data-shared-prod.org_mozilla_firefox.baseline_clients_first_seen`",  # noqa: E501
            )
            Segment_list.append(
                Segment(
                    name="clients_last_seen_filter",
                    data_source=baseline_clients_first_seen,
                    select_expr=f"COALESCE(MIN(first_seen_date)  >= '{start_date}', TRUE)",
                )
            )
        elif target["user_type"] == "existing":
            baseline_clients_last_seen = SegmentDataSource(
                name="baseline_clients_last_seen",
                from_expr="`moz-fx-data-shared-prod.org_mozilla_firefox.baseline_clients_last_seen`",  # noqa: E501
            )
            Segment_list.append(
                Segment(
                    name="clients_last_seen_filter",
                    data_source=baseline_clients_last_seen,
                    select_expr="""COALESCE(MIN(first_seen_date) <= '{first_day}', TRUE)
                    AND COALESCE(MIN(days_since_seen) = 0)""".format(
                        first_day=add_days(start_date, -28)
                    ),
                )
            )

        return Segment_list

    def _fenix_sql(self, target: Dict) -> str:
        clients_daily_sql = """
        COALESCE(LOGICAL_OR(
        (normalized_channel = '{channel}') AND
        (UPPER(locale) in {locale}) AND
        (country = '{country}')
        )
        )
        """.format(
            channel=target["release_channel"],
            locale=target["locale"],
            country=target["country"],
        )

        return clients_daily_sql


@attr.s(auto_attribs=True)
class MetricsLists:
    def from_file(self, target_dict: Dict, path: TextIO) -> List[Metric]:
        if "metrics" not in target_dict.keys():
            raise MetricsTagNotFoundException(path)

        metrics_dict = target_dict["metrics"]

        if "import_from_metric_hub" in metrics_dict.keys():
            for app_id, metrics in metrics_dict["import_from_metric_hub"].items():
                for metric in metrics:
                    metrics_dict[metric] = ConfigLoader.get_metric(metric, app_id)
            metrics_dict.pop("import_from_metric_hub")

        if (
            "data_sources" in target_dict.keys()
            and "import_from_metric_hub" in target_dict["data_sources"].keys()
        ):
            for app_id, data_sources in target_dict["data_sources"][
                "import_from_metric_hub"
            ].items():
                for data_source in data_sources:
                    target_dict["data_sources"][data_source] = ConfigLoader.get_data_source(
                        data_source, app_id
                    )

        Metric_list = []
        for key, value in metrics_dict.items():
            if isinstance(value, Metric):
                Metric_list.append(value)
            else:
                data_source = target_dict["data_sources"][value["data_source"]]
                Metric_list.append(
                    Metric(
                        name=key,
                        data_source=DataSource(
                            name=value["data_source"], from_expr=data_source["from_expression"]
                        ),
                        select_expr=ConfigLoader.configs.get_env()
                        .from_string(value["select_expression"])
                        .render(),
                    )
                )

        return Metric_list

    def from_repo(self, target_dict: Dict, app_id: ALLOWED_APPS) -> List[Metric]:
        metric_names = target_dict["metrics"][app_id]
        Metric_list = []

        for metric in metric_names:
            Metric_list.append(ConfigLoader.get_metric(metric, app_id))

        return Metric_list


@attr.s(auto_attribs=True)
class SizingConfiguration:
    target_list: List[Segment]
    target_slug: str
    metric_list: List[Metric]
    start_date: str
    num_dates_enrollment: int
    analysis_length: int
    parameters: List[Dict]
    config_file: Optional[TextIO] = None


@attr.s(auto_attribs=True)
class SizingCollection:
    sizing_targets: List[Segment] = attr.Factory(list)
    sizing_metrics: List[Metric] = attr.Factory(list)
    sizing_parameters: List[Dict] = attr.Factory(list)
    sizing_dates: Dict = attr.Factory(dict)
    segments_list = SegmentsList()
    metrics_list = MetricsLists()

    @classmethod
    def from_repo(
        cls,
        target: Dict,
        jobs_dict: Dict,
        app_id: ALLOWED_APPS = "firefox_desktop",
    ) -> "SizingCollection":
        dates_dict = default_dates_dict(datetime.today())
        segments_list = cls.segments_list.from_repo(
            target, app_id, dates_dict["start_date"]  # type: ignore[arg-type]
        )
        metric_list = cls.metrics_list.from_repo(jobs_dict, app_id)

        parameters_list = dict_combinations(jobs_dict, "parameters")

        return cls(segments_list, metric_list, parameters_list, dates_dict)

    @classmethod
    def from_file(cls, path: TextIO) -> "SizingCollection":
        target_dict = toml.load(path)

        segment_list = cls.segments_list.from_file(target_dict, path)
        metric_list = cls.metrics_list.from_file(target_dict, path)

        if "parameters" in target_dict.keys():
            parameters_list = dict_combinations(target_dict["parameters"], "sizing")
            dates_dict = target_dict["parameters"]["dates"]
        else:
            parameters_dict = {
                "parameters": {"power": [0.8], "effect_size": [0.005, 0.01, 0.02, 0.05]}
            }
            parameters_list = dict_combinations(parameters_dict, "parameters")

            dates_dict = default_dates_dict(datetime.today())

        return cls(segment_list, metric_list, parameters_list, dates_dict)
