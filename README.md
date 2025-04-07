![pydbsmgr](https://raw.githubusercontent.com/jzsmoreno/pydbsmgr/main/pydbsmgr.png)

![GitHub last commit](https://img.shields.io/github/last-commit/jzsmoreno/pydbsmgr?style=for-the-badge)
![GitHub repo size](https://img.shields.io/github/repo-size/jzsmoreno/pydbsmgr?style=for-the-badge)
![License](https://img.shields.io/github/license/jzsmoreno/pydbsmgr?style=for-the-badge)
[![CI - Test](https://github.com/jzsmoreno/pydbsmgr/actions/workflows/python-app.yml/badge.svg)](https://github.com/jzsmoreno/pydbsmgr/actions/workflows/python-app.yml)

# pydbsmgr

Python package for database control and administration as well as for DataFrame processing. Also contains a simpler [Python SDK for Azure Cloud tools](https://learn.microsoft.com/en-us/azure/developer/python/sdk/azure-sdk-overview) such as uploading `.parquet`, `.csv` and `.xlsx` files, downloading them and DataFrame cleansing, for more information review our [`documentation`](https://jzsmoreno.github.io/pydbsmgr/).

## Prerequisites

Before you begin, ensure you have met the following requirements:

- You have a _Windows/Linux/Mac_ machine running [Python 3.10+](https://www.python.org/).
- You have installed the latest versions of [`pip`](https://pip.pypa.io/en/stable/installing/) and [`virtualenv`](https://virtualenv.pypa.io/en/stable/installation/) or `conda` ([Anaconda](https://www.anaconda.com/distribution/)).

## Installation

This package can be easily installed with pip:

```bash
pip install pydbsmgr
```

## Installation from GitHub

If you prefer, you can do it in this other way:

```bash
pip install git+https://github.com/jzsmoreno/pydbsmgr
```

## You can contribute to this repository

Through the following steps:

1. Clone this repository (requires `git`):

```
git clone https://github.com/jzsmoreno/pydbsmgr.git
cd pydbsmgr
```

1. Create and activate a python virtual environment (The activation may vary depending on your OS here is the example for Linux):

```
python3 -m venv venv
source ./venv/bin/activate
```

3. Install all the `requirements`:

```
pip3 install -r requirements.txt
```

4. Now you can test it with any python file you want. But you can also use it in a jupyter notebook, just install [jupyterlab](https://pypi.org/project/jupyterlab/)

```
pip3 install jupyterlab
```

5. Open the jupyter lab instance (you can see that already exists an `example.ipynb`, where you can see some of the things you can do):

```
jupyter lab
```
