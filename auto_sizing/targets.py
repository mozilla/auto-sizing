from typing import List, Dict
from pathlib import Path
import toml
from datetime import datetime
import attr
from mozanalysis.segments import Segment, SegmentDataSource
from mozanalysis.metrics import Metric, DataSource
from mozanalysis.config import ConfigLoader

from .utils import dict_combinations, default_dates_dict
from .errors import (
    MetricsTagNotFoundException,
    SegmentsTagNotFoundException,
    SegmentDataSourcesTagNotFoundException,
    DataSourcesTagNotFoundException,
)


class SegmentsList:
    """Builds list of Segments from list of dictionaries"""

    def from_list(self, target_list: List[Dict]) -> List[Segment]:
        return self._make_desktop_targets(target_list)

    def from_file(self, target_dict: Dict, path: str) -> List[Segment]:

        if "segments" not in target_dict.keys():
            raise SegmentsTagNotFoundException(path)

        segments_dict = target_dict["segments"]
        if "data_sources" not in segments_dict.keys():
            raise SegmentDataSourcesTagNotFoundException(path)

        if "import_from_metric_hub" in segments_dict.keys():
            for app_id, segments in segments_dict["import_from_metric_hub"].items():
                for segment in segments:
                    segments_dict[segment] = ConfigLoader.get_segment(segment, app_id)
            segments_dict.pop("import_from_metric_hub")

        if "import_from_metric_hub" in segments_dict["data_sources"].keys():
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

    def _make_desktop_targets(self, target_list: List[Dict[str, str]]) -> List[Segment]:

        clients_daily = ConfigLoader.get_segment_data_source("clients_daily", "firefox_desktop")
        Segment_list = []

        for recipe in target_list:
            Segment_list.append(
                Segment(
                    name="",
                    data_source=clients_daily,
                    select_expr=self._target_to_sql(recipe),
                )
            )

        return Segment_list

    def _target_to_sql(self, recipe: str) -> str:
        return """
        COALESCE(LOGICAL_OR(
        (mozfun.norm.truncate_version(app_display_version, 'major') >= {version}) AND
        (normalized_channel = {channel}) AND
        (locale in {locale}) AND
        (country = {country})
        )
        )
        """.format(
            version=recipe["minimum_version"],
            channel=recipe["release_channel"],
            locale=recipe["locale"],
            country=recipe["country"],
        )


@attr.s(auto_attribs=True)
class MetricsLists:
    def from_file(self, target_dict: Dict, path: str) -> List[Metric]:

        if "metrics" not in target_dict.keys():
            raise MetricsTagNotFoundException(path)

        metrics_dict = target_dict["metrics"]
        if "data_sources" not in target_dict.keys():
            raise DataSourcesTagNotFoundException(path)

        if "import_from_metric_hub" in metrics_dict.keys():
            for app_id, metrics in metrics_dict["import_from_metric_hub"].items():
                for metric in metrics:
                    metrics_dict[metric] = ConfigLoader.get_metric(metric, app_id)
            metrics_dict.pop("import_from_metric_hub")

        if "import_from_metric_hub" in target_dict["data_sources"].keys():
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


@attr.s(auto_attribs=True)
class SizingConfiguration:
    target_list: List[Segment]
    target_slug: str
    metric_list: List[Metric]
    start_date: datetime
    num_dates_enrollment: int
    analysis_length: int
    parameters: List[Dict]
    config_file: str


@attr.s(auto_attribs=True)
class SizingCollection:

    sizing_targets: List[Segment] = attr.Factory(list)
    sizing_metrics: List[Metric] = attr.Factory(list)
    sizing_parameters: List[Dict] = attr.Factory(list)
    sizing_dates: Dict = attr.Factory(dict)
    toml_path = Path(__file__).parent / "data/target_lists.toml"
    segments_list = SegmentsList()
    metrics_list = MetricsLists()

    @classmethod
    def from_repo(cls, app_id: str = "firefox_desktop") -> "SizingCollection":

        jobs_dict = toml.load(cls.toml_path)
        target_list = dict_combinations(jobs_dict, "targets")
        segments_list = cls.segments_list.from_list(target_list)

        metric_names_list = jobs_dict["metrics"][app_id]
        metric_list = [ConfigLoader.get_metric(metric, app_id) for metric in metric_names_list]

        parameters_list = dict_combinations(jobs_dict, "parameters")

        dates_dict = default_dates_dict(datetime.today())

        return cls(segments_list, metric_list, parameters_list, dates_dict)

    @classmethod
    def from_file(cls, path: str) -> "SizingCollection":

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
