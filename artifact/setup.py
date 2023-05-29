import setuptools

REQUIRED_PACKAGES = [
    "google-cloud-storage==2.9.0",
    "google-cloud-bigquery==3.10.0",
    "apache-beam[gpc]==2.4.00",
    "pandas==2.0.1",
    "geopandas==0.13.0",
    "numpy==1.24.3",
    "regex==2023.5.5",
    "pyarrow==11.0.0",
]

setuptools.setup(
    name="extractsetup",
    version="0.0.1",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    include_package_data=True,
)
