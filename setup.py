from setuptools import setup


def text_from_file(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


test_dependencies = [
    "coverage",
    "isort",
    "jsonschema",
    "pytest",
    "pytest-black",
    "pytest-cov",
    "pytest-flake8",
    "mypy",
    "types-futures",
    "types-pkg-resources",
    "types-protobuf",
    "types-pytz",
    "types-PyYAML",
    "types-requests",
    "types-six",
    "types-toml",
]

extras = {
    "testing": test_dependencies,
}

setup(
    name="mozilla-auto-sizing",
    author="Mozilla Corporation",
    author_email="fx-data-dev@mozilla.org",
    description="Runs automatic sample size calc",
    url="https://github.com/m-d-bowerman/auto-sizing",
    packages=[
        "auto_sizing",
        "auto_sizing.logging",
        "auto_sizing.tests",
        "auto_sizing.workflows",
    ],
    package_data={
        "auto_sizing.workflows": ["*.yaml"],
        "auto_sizing": ["data/*", "../*.toml"],
    },
    install_requires=[
        "attrs",
        "cattrs",
        "Click",
        "dask[distributed]",
        "db-dtypes",
        "GitPython",
        "google-cloud-bigquery",
        "google-cloud-bigquery-storage",
        "google-cloud-container",
        "google-cloud-storage",
        "grpcio",  # https://github.com/googleapis/google-cloud-python/issues/6259
        "jinja2",
        "mozanalysis",
        "mozilla-jetstream",
        "mozilla-metric-config-parser",
        "pyarrow",
        "pytz",
        "PyYAML",
        "requests",
        "smart_open[gcs]",
        "statsmodels",
        "toml",
    ],
    include_package_data=True,
    tests_require=test_dependencies,
    extras_require=extras,
    long_description=text_from_file("README.md"),
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    entry_points="""
            [console_scripts]
            pensieve=auto_sizing.cli:cli
            auto_sizing=auto_sizing.cli:cli
        """,
    # This project does not issue releases, so this number is not meaningful
    # and should not need to change.
    version="2023.2.0",
)
