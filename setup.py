from setuptools import setup, find_packages


DESCRIPTION = (
    "Tools for MuseumPlus digital preservation processes"
)
LONG_DESCRIPTION = DESCRIPTION
AUTHOR = "Janne Pulkkinen"
AUTHOR_EMAIL = "janne.pulkkinen@museovirasto.fi"


setup(
    name="passari",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    packages=find_packages("src"),
    include_package_data=True,
    package_dir={"passari": "src/passari"},
    install_requires=[
        "aiohttp", "aiofiles", "lxml",
        "click>=7", "click<8",
        "toml", "python-dateutil", "paramiko", "filelock"
    ],
    entry_points={
        "console_scripts": [
            "download-object = passari.scripts.download_object:cli",
            "create-sip = passari.scripts.create_sip:cli",
            "submit-sip = passari.scripts.submit_sip:cli",
            "confirm-sip = passari.scripts.confirm_sip:cli"
        ]
    },
    use_scm_version=True,
    setup_requires=["setuptools_scm"]
)
