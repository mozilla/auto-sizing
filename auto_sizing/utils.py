import itertools
from datetime import datetime, timedelta
from typing import Dict, List, Union


def dict_combinations(dictionary: Dict, key: str) -> List[Dict[str, Union[List, Dict]]]:

    keys, values = zip(*dictionary[key].items())
    dictionary_list = [dict(zip(keys, v)) for v in itertools.product(*values)]

    return dictionary_list


def default_dates_dict(
    current_date: datetime, num_dates_enrollment: int = 7, analysis_length: int = 28
) -> Dict[str, Union[int, str]]:

    start_date = current_date - timedelta(num_dates_enrollment + analysis_length + 1)

    return {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "num_dates_enrollment": num_dates_enrollment,
        "analysis_length": analysis_length,
    }
