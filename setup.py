import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pydbsmgr",
    version="0.1.5",
    author="J. A. Moreno-Guerra",
    author_email="jzs.gm27@gmail.com",
    description="Testing installation of Package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jzsmoreno/pydbsmgr",
    project_urls={"Bug Tracker": "https://github.com/jzsmoreno/pydbsmgr"},
    license="MIT",
    packages=["pydbsmgr"],
    install_requires=[
        "numpy",
        "pandas",
        "clean-text",
        "missingno",
        "pyodbc",
        "ipython",
        "SQLAlchemy",
        "pyyaml",
        "pyarrow",
        "fastparquet"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
