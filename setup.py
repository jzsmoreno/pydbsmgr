from pathlib import Path

import setuptools
from pip._internal.req import parse_requirements

# Parse the requirements.txt file
requirements = parse_requirements("requirements.txt", session="hack")

# Get the list of requirements as strings
install_requires = [str(req.requirement) for req in requirements]

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

about = {}
ROOT_DIR = Path(__file__).resolve().parent
PACKAGE_DIR = ROOT_DIR / "pydbsmgr"
with open(PACKAGE_DIR / "VERSION") as f:
    _version = f.read().strip()
    about["__version__"] = _version


setuptools.setup(
    name="pydbsmgr",
    version=about["__version__"],
    author="J. A. Moreno-Guerra",
    author_email="jzs.gm27@gmail.com",
    maintainer="David Pedroza",
    maintainer_email="david.pedroza.segoviano@gmail.com",
    description="Python package for database control and DataFrame processing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jzsmoreno/pydbsmgr",
    project_urls={"Bug Tracker": "https://github.com/jzsmoreno/pydbsmgr"},
    license="MIT",
    packages=setuptools.find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
)
