from setuptools import find_packages, setup

setup(
    name="orchestrator",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "dbt-postgres==1.10.0",
        "dagster==1.12.22",
        "dagster-aws==0.28.22",
        "dagster-dbt==0.28.22",
        "dagster-pipes",
        "dagster-pandas==0.28.22",
        "dagster-pandera==0.28.22",
        "dagster-postgres==0.28.22",
        "playwright",
    ],
    extras_require={
        "dev": ["dagster-webserver==1.12.22", "pytest"],
        "tests": ["pytest", "pytest-mock"],
    },
)
