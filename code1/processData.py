"""
Loads and processes data, adding flags, and saves it to a CSV file.
"""

import datetime
import logging
from datetime import datetime as dt
from pathlib import Path
# Third-party packages
from IPython import get_ipython
import json
import pandas as pd
# Local packages
from drapi.drapi import getTimestamp, successiveParents, make_dir_path
from appleHealthExport.code.functions import parseExportFile, getRecordTypes, getRecordsByAttributeValue, tabulateRecords, time2ordinal
from code1.functions import parseTimes, labelByDatetimeSpan

# Arguments
DATA_FILE_PATH = Path("data/input/apple_health_export/export.xml")

PROJECT_DIR_DEPTH = 2

LOG_LEVEL = "INFO"

# Settings: Interactive Pyplot
get_ipython().run_line_magic("matplotlib", "")

# Variables: Path construction: General
runTimestamp = getTimestamp()
thisFilePath = Path(__file__)
thisFileStem = thisFilePath.stem
projectDir, _ = successiveParents(thisFilePath.absolute(), PROJECT_DIR_DEPTH)
dataDir = projectDir.joinpath("data")
if dataDir:
    inputDataDir = dataDir.joinpath("input")
    outputDataDir = dataDir.joinpath("output")
    if outputDataDir:
        runOutputDir = outputDataDir.joinpath(thisFileStem, runTimestamp)
logsDir = projectDir.joinpath("logs")
if logsDir:
    runLogsDir = logsDir.joinpath(thisFileStem)

# Variables: Path construction: Project-specific
pass

# Variables: Other
pass

# Directory creation: General
make_dir_path(runOutputDir)
make_dir_path(runLogsDir)

# Directory creation: Project-specific
pass

# Logging block
logpath = runLogsDir.joinpath(f"log {runTimestamp}.log")
# logFormat = logging.Formatter(f"""[%(asctime)s]\n[%(levelname)s](%(funcName)s)\n{"-"*24}> %(message)s""")
logFormat = logging.Formatter("""[%(asctime)s][%(levelname)s](%(funcName)s): %(message)s""")

logger = logging.getLogger(__name__)

fileHandler = logging.FileHandler(logpath)
fileHandler.setLevel(9)
fileHandler.setFormatter(logFormat)

streamHandler = logging.StreamHandler()
streamHandler.setLevel(LOG_LEVEL)
streamHandler.setFormatter(logFormat)

logger.addHandler(fileHandler)
logger.addHandler(streamHandler)

logger.setLevel(9)

if __name__ == "__main__":
    logger.info(f"""Begin running "{thisFilePath}".""")
    logger.info(f"""All other paths will be reported in debugging relative to `projectDir`: "{projectDir}".""")
    logger.info(f"""Script arguments:

    # Arguments
    `DATA_FILE_PATH`: "{DATA_FILE_PATH}"

    # Arguments: General
    `PROJECT_DIR_DEPTH`: "{PROJECT_DIR_DEPTH}"

    `LOG_LEVEL` = "{LOG_LEVEL}"
    """)

    # Parse data
    tree = parseExportFile(DATA_FILE_PATH)

    # Get record types
    recordTypes = getRecordTypes(tree=tree)

    # Get systolic and diastolic blood pressure records
    recordsSBP = getRecordsByAttributeValue(tree=tree,
                                            attribute="type",
                                            value="HKQuantityTypeIdentifierBloodPressureSystolic")
    recordsDBP = getRecordsByAttributeValue(tree=tree,
                                            attribute="type",
                                            value="HKQuantityTypeIdentifierBloodPressureDiastolic")

    # Tabulate blood pressure
    dfSBP = tabulateRecords(records=recordsSBP)
    dfDBP = tabulateRecords(records=recordsDBP)

    # Analysis pre-processing
    TABLES_TO_PROCESS = {"Systolic BP": dfSBP,
                         "Diastolic BP": dfDBP}
    for tableName, table in TABLES_TO_PROCESS.items():
        table = parseTimes(pdObject=table)

    # Identify groups by medication start and end times.
    AMLODAPINE_START_DATETIME = pd.to_datetime("2023-04-26 15:04:00-04:00")
    AMLODAPINE_STOP_DATETIME = pd.to_datetime("2023-06-11 02:28:00-04:00")
    LOSARTAN_START_DATETIME = pd.to_datetime("2023-06-11 02:28:00-04:00")
    LOSARTAN_STOP_DATETIME = pd.to_datetime(dt.now())
    MEDICATION_DATETIMES = {"Amlodapine Potassium": {"start": AMLODAPINE_START_DATETIME,
                                                     "stop": AMLODAPINE_STOP_DATETIME},
                            "Losartan Potassium": {"start": LOSARTAN_START_DATETIME,
                                                   "stop": LOSARTAN_STOP_DATETIME}}

    TABLES_TO_PROCESS, qaTables = labelByDatetimeSpan(tablesToProcess=TABLES_TO_PROCESS,
                                                      labelDatetimes=MEDICATION_DATETIMES,
                                                      troubleshooting=True,
                                                      logger=logger)

    # Identify subgroups by time of day
    GROUP_1_START_TIME = pd.to_datetime("03:00:00-04:00")
    GROUP_1_STOP_TIME = pd.to_datetime("12:00:00-04:00")
    GROUP_2_START_TIME = pd.to_datetime("12:00:00-04:00")
    GROUP_2_STOP_TIME = pd.to_datetime("03:00:00-04:00")
    GROUP_TIMES = {"Group 1 (Morning)": {"start": GROUP_1_START_TIME,
                                         "stop": GROUP_1_STOP_TIME},
                   "Group 2 (Evening)": {"start": GROUP_2_START_TIME,
                                         "stop": GROUP_2_STOP_TIME}}

    midnight1 = datetime.datetime(2023, 7, 9, 23, 59, 59, (10**6) - 1).time()
    midnight2 = datetime.datetime(2023, 7, 9, 0, 0, 0, 0).time()
    m1 = time2ordinal(midnight1)
    m2 = time2ordinal(midnight2)
    for tableName, table in TABLES_TO_PROCESS.items():
        for group, timeDict in GROUP_TIMES.items():
            startTime = timeDict["start"]
            stopTime = timeDict["stop"]
            t0 = time2ordinal(startTime)
            t1 = time2ordinal(stopTime)
            startDateOrdinal = table["startDate"].apply(lambda ts: time2ordinal(ts))
            endDateOrdinal = table["endDate"].apply(lambda ts: time2ordinal(ts))
            if t0 > t1:
                pass  # TODO
                flagStartTime1 = (t0 <= startDateOrdinal) & (startDateOrdinal <= m1)
                flagStartTime2 = (m2 <= startDateOrdinal) & (startDateOrdinal <= t1)
                flagEndTime1 = (t0 < endDateOrdinal) & (endDateOrdinal <= m1)
                flagEndTime2 = (m2 <= endDateOrdinal) & (endDateOrdinal < t1)
                flagGroup = (flagStartTime1 | flagStartTime2) & (flagEndTime1 | flagEndTime2)
            else:
                flagStartTime = (t0 <= startDateOrdinal) & (startDateOrdinal <= t1)
                flagEndTime = (t0 < endDateOrdinal) & (endDateOrdinal < t1)
                flagGroup = flagStartTime & flagEndTime
            table[group] = flagGroup

        # Perform QA
        allGroups = [group for group in GROUP_TIMES.keys()]
        table["QA: Unassigned (Groups)"] = ~table[allGroups].any()
        logger.info(f"""All observations should be assigned to a group. The current table has {table["QA: Unassigned (Groups)"].sum()} unassigned observations.""")

    # Save tables
    tablesDir = runOutputDir.joinpath("tablesToProcess")
    make_dir_path(tablesDir)
    for tableName, table in TABLES_TO_PROCESS.items():
        savepath = tablesDir.joinpath(f"{tableName}.CSV")
        table.to_csv(savepath, index=False)

    # Save group and medication objects
    jsonDir = runOutputDir.joinpath("jsonDir")
    make_dir_path(jsonDir)

    groupsPath = jsonDir.joinpath("allGroups.JSON")
    with open(groupsPath, "w") as file:
        file.write(json.dumps(allGroups))

    qaTablesPath = jsonDir.joinpath("qaTables.JSON")
    with open(qaTablesPath, "w") as file:
        file.write(json.dumps(qaTables))

    logger.info(f"""All results saved to "{runOutputDir.absolute().relative_to(projectDir)}".""")

    # End script
    logger.info(f"""Finished running "{thisFilePath.absolute().relative_to(projectDir)}".""")
