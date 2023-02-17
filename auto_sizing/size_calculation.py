from mozanalysis.sizing import HistoricalTarget
from mozanalysis.experiment import TimeLimits
from mozanalysis.bq import BigQueryContext, sanitize_table_name_for_bq
from mozanalysis.frequentist_stats.sample_size import (
    z_or_t_ind_sample_size_calc,
)

import auto_sizing.errors as errors
from auto_sizing.export_json import export_sample_size_json
from auto_sizing.targets import SizingConfiguration
from auto_sizing.utils import delete_bq_table

from datetime import datetime, timedelta
from pathlib import Path
import attr
from typing import Any, Mapping, Dict, List, Optional, Tuple
import json
from pandas import DataFrame


@attr.s(auto_attribs=True)
class SizeCalculation:
    """Wrapper for size calculation for target recipe."""

    project: str
    dataset: str
    bucket: str
    config: SizingConfiguration

    @property
    def bigquerycontext(self):
        return BigQueryContext(project_id=self.project, dataset_id=self.dataset)

    def _validate_requested_timelimits(self, current_date: datetime) -> Optional[TimeLimits]:
        """
        Checks if requested dates of data are available and not in the future.
        Returns a TimeLimits instance if possible; else, returns None.
        """

        last_date_full_data = datetime.strptime(self.config.start_date, "%Y-%m-%d") + timedelta(
            days=(self.config.num_dates_enrollment + self.config.analysis_length - 1)
        )

        if last_date_full_data.date() >= current_date:
            raise errors.AnalysisDatesNotAvailableException(self.config.target_slug)

        return TimeLimits.for_single_analysis_window(
            self.config.start_date,
            last_date_full_data.strftime("%Y-%m-%d"),
            0,
            self.config.analysis_length,
            self.config.num_dates_enrollment,
        )

    def calculate_metrics(
        self,
        time_limits: TimeLimits,
        ht: HistoricalTarget,
    ) -> Tuple[DataFrame, str]:
        targets_sql = ht.build_targets_query(
            time_limits=time_limits,
            target_list=self.config.target_list,
        )

        targets_table_name = sanitize_table_name_for_bq(
            "_".join(
                [
                    "auto-sizing",
                    self.config.target_slug,
                ]
            )
        )

        self.bigquerycontext.run_query(targets_sql, targets_table_name, replace_tables=True)

        metrics_sql = ht.build_metrics_query(
            time_limits=time_limits,
            metric_list=self.config.metric_list,
            targets_table=self.bigquerycontext.fully_qualify_table_name(targets_table_name),
        )

        metrics_table_name = sanitize_table_name_for_bq(
            "_".join(
                [
                    "metrics-table",
                    self.config.target_slug,
                ]
            )
        )

        df = self.bigquerycontext.run_query(metrics_sql, metrics_table_name).to_dataframe()
        delete_bq_table(
            self.bigquerycontext.fully_qualify_table_name(targets_table_name), self.project
        )

        return df, metrics_table_name

    def calculate_sample_sizes(
        self, metrics_table: DataFrame, parameters: Dict[str, float]
    ) -> Dict[str, int]:
        res = z_or_t_ind_sample_size_calc(
            df=metrics_table,
            metrics_list=self.config.metric_list,
            effect_size=parameters["effect_size"],
            power=parameters["power"],
        )

        result_dict = {
            key: {
                "number_of_clients_targeted": res[key]["number_of_clients_targeted"],
                "population_percent_per_branch": res[key]["population_percent_per_branch"],
            }
            for key in res.keys()
        }
        result_dict["parameters"] = parameters

        return result_dict

    def publish_results(self, result_dict: Dict[str, float]) -> None:
        if self.config.config_file and not self.bucket:
            path = Path(self.config.config_file.name).parent / f"{self.config.target_slug}.json"
            path.write_text(json.dumps(result_dict))
            print(f"Results saved at {path}")

        else:
            export_sample_size_json(
                self.project, self.bucket, self.config.target_slug, json.dumps(result_dict)
            )

    def run(
        self,
        current_date: datetime,
    ) -> None:
        time_limits = self._validate_requested_timelimits(current_date)

        ht = HistoricalTarget(
            experiment_name=self.config.target_slug,
            start_date=self.config.start_date,
            analysis_length=self.config.analysis_length,
            num_dates_enrollment=self.config.num_dates_enrollment,
        )

        metrics_table, metrics_table_name = self.calculate_metrics(time_limits=time_limits, ht=ht)
        print(f"Metrics table saved at {metrics_table_name}")

        results_combined = {}

        if len(metrics_table) == 0:
            print("No clients satisfied targeting.")
            return 0

        for parameters in self.config.parameters:
            res = self.calculate_sample_sizes(metrics_table=metrics_table, parameters=parameters)
            res["parameters"] = parameters
            results_combined[
                f"Power{str(parameters['power'])}EffectSize{str(parameters['effect_size'])}"
            ] = res

        self.publish_results(results_combined)
