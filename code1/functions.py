"""
Functions for this module
"""

from typing import Union
# Third-party packages
import pandas as pd


def parseTimes(pdObject: Union[pd.Series, pd.DataFrame]):
    """
    Converts table columns to pandas datetime objects.
    """
    timeColumns = ["creationDate",
                   "startDate",
                   "endDate"]
    if isinstance(pdObject, pd.Series):
        result = pd.to_datetime(pdObject)
    elif isinstance(pdObject, pd.DataFrame):
        for column in pdObject.columns:
            if column in timeColumns:
                pdObject[column] = pd.to_datetime(pdObject[column])
        result = pdObject
    else:
        raise Exception(f"""Unexpected object of type "{type(pdObject)}"". Expected objects of type pd.Series or pd.DataFrame.""")
    return result
