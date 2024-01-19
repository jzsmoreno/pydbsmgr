import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pydbsmgr",
    version="0.8.2",
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
    install_requires=[
        "numpy",
        "pandas",
        "clean-text",
        "missingno",
        "pyodbc",
        "ipython",
        "SQLAlchemy",
        "pyyaml",
        "azure-storage-blob==12.16.0",
        "python-dotenv==1.0.0",
        "openpyxl==3.1.2",
        "pyarrow",
        "fastparquet",
        "loguru",
        "psutil",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
)
