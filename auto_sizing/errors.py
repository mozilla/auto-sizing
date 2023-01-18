class ValidationException(Exception):
    """Exception thrown when an experiment is invalid."""

    def __init__(self, message):
        super().__init__(message)


class AnalysisDatesNotAvailableException(ValidationException):
    def __init__(self, message="Analysis period extends into the future."):
        super().__init__(f"{message}")


class SegmentsTagNotFoundException(ValidationException):
    def __init__(self, path, message="No `segments` tag found in config file."):
        super().__init__(f"{path} -> {message}")


class MetricsTagNotFoundException(ValidationException):
    def __init__(self, path, message="No `metrics` tag found in config file."):
        super().__init__(f"{path} -> {message}")


class DataSourcesTagNotFoundException(ValidationException):
    def __init__(self, path, message="No `data_sources` tag found in config file."):
        super().__init__(f"{path} -> {message}")


class SegmentDataSourcesTagNotFoundException(ValidationException):
    def __init__(self, path, message="No `segments.data_sources` tag found in config file."):
        super().__init__(f"{path} -> {message}")


class NoConfigFileException(ValidationException):
    def __init__(self, message="Provide a TOML config file."):
        super().__init__(f"{message}")
