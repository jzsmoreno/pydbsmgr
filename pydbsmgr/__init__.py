"""
pydbsmgr: Initialize the Package
=====================================

This is the entry point of pydbsmgr, your comprehensive database management companion. It initializes all necessary modules and provides a central hub for accessing various tools and functions.

Main Modules:
- pydbsmgr.main: Provides access to core functionality, including data manipulation, query execution, and database operations.
- pydbsmgr.utils.azure_sdk: Offers integration with Azure SDK for seamless interaction with Microsoft's cloud-based services.
- pydbsmgr.utils.tools: Contains utility functions for data processing, normalization, and visualization.

By importing the main modules directly or accessing them through this central entry point (i.e., from pydbsmgr import *), you can leverage the full range of pydbsmgr's capabilities to streamline your database management workflow.
"""

from pydbsmgr.main import *
from pydbsmgr.utils.azure_sdk import *
from pydbsmgr.utils.tools import *
