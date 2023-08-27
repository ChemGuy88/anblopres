"""
Functions for this module
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Dict, Union, cast
# Third-party packages
import pandas as pd


# class Logger(logging.Logger):
#     """
#     Hack for type hinting `logging` `Logger` objects.
#     h/t https://github.com/python/typeshed/issues/1801
#     """
#     def foo(self):
#         pass


# logging.setLoggerClass(Logger)
# logger = cast(Logger, logging.getLogger(__name__))


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


def labelByDatetimeSpan(tablesToProcess: Dict[str, pd.DataFrame],
                        labelDatetimes: Dict[str, dt.datetime],
                        troubleshooting: bool,
                        logger: logging.Logger):
    """
    Creates labels for medical records based on the beginning and end of a period of time.
    NOTE: Start times are inclusive. End times are not inclusive.
    I.e., if an object to be labeled is between both a stop and start time, it should
    be assigned to the start time's group.
    """
    for tableName, table in tablesToProcess.items():
        for labelName, datetimeDict in labelDatetimes.items():
            startDatetime = datetimeDict["start"]
            stopDatetime = datetimeDict["stop"]
            t0ordinal = startDatetime.toordinal()
            t1ordinal = stopDatetime.toordinal()
            startDateOrdinal = table["startDate"].apply(lambda ts: ts.toordinal())
            endDateOrdinal = table["endDate"].apply(lambda ts: ts.toordinal())
            labelStartDatetime = (t0ordinal <= startDateOrdinal) & (startDateOrdinal <= t1ordinal)
            labelEndDatetime = (t0ordinal < endDateOrdinal) & (endDateOrdinal < t1ordinal)
            label = labelStartDatetime & labelEndDatetime
            table[labelName] = label
        allLabels = [group for group in labelDatetimes.keys()]
        table["QA: Unassigned Labels"] = ~table[allLabels].any()
        logger.info(f"""All observations should be assigned to a group. The current table has {table["QA: Unassigned Labels"].sum()} unassigned observations.""")

    if troubleshooting:
        return tablesToProcess, allLabels
    else:
        return tablesToProcess
