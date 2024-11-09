from setuptools import find_packages, setup

setup(
    name="ra",
    packages=find_packages(exclude=["ra_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
