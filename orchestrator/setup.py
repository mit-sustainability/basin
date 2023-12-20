from setuptools import find_packages, setup

setup(
    name="orchestrator",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "dbt-postgres",
        "dagster",
        "dagster-pandas",
        "dagster-pandera",
        "dagster-dbt",
        "dagster-aws",
    ],
    extras_require={
        "dev": ["dagster-webserver", "pytest"],
        "tests": ["pytest"],
    },
)
