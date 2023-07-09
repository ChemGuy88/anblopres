"""
Analysis of blood pressure exported from Apple Health
"""

import datetime
import logging
from datetime import datetime as dt
from pathlib import Path
# Third-party packages
from IPython import get_ipython
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import statsmodels.api as sm
# Local packages
from drapi.drapi import getTimestamp, successiveParents, make_dir_path
from appleHealthExport.code.functions import parseExportFile, getRecordTypes, getRecordsByAttributeValue, tabulateRecords

# Arguments
DATA_FILE_PATH = Path("data/input/apple_health_export/export.xml")

PROJECT_DIR_DEPTH = 2
IRB_DIR_DEPTH = PROJECT_DIR_DEPTH + 1
IDR_DATA_REQUEST_DIR_DEPTH = IRB_DIR_DEPTH + 3

LOG_LEVEL = "INFO"

# Settings: Interactive Pyplot
get_ipython().run_line_magic("matplotlib", "")

# Variables: Path construction: General
runTimestamp = getTimestamp()
thisFilePath = Path(__file__)
thisFileStem = thisFilePath.stem
projectDir, _ = successiveParents(thisFilePath.absolute(), PROJECT_DIR_DEPTH)
IRBDir, _ = successiveParents(thisFilePath, IRB_DIR_DEPTH)
IDRDataRequestDir, _ = successiveParents(thisFilePath.absolute(), IDR_DATA_REQUEST_DIR_DEPTH)
dataDir = projectDir.joinpath("data")
if dataDir:
    inputDataDir = dataDir.joinpath("input")
    outputDataDir = dataDir.joinpath("output")
    if outputDataDir:
        runOutputDir = outputDataDir.joinpath(thisFileStem, runTimestamp)
logsDir = projectDir.joinpath("logs")
if logsDir:
    runLogsDir = logsDir.joinpath(thisFileStem)
sqlDir = projectDir.joinpath("sql")

# Variables: Path construction: Project-specific
pass

# Variables: Other
pass

# Directory creation: General
make_dir_path(runOutputDir)
make_dir_path(runLogsDir)

# Directory creation: Project-specific
pass


if __name__ == "__main__":
    # Logging block
    logpath = runLogsDir.joinpath(f"log {runTimestamp}.log")
    fileHandler = logging.FileHandler(logpath)
    fileHandler.setLevel(LOG_LEVEL)
    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(LOG_LEVEL)

    logging.basicConfig(format="[%(asctime)s][%(levelname)s](%(funcName)s): %(message)s",
                        handlers=[fileHandler, streamHandler],
                        level=LOG_LEVEL)

    logging.info(f"""Begin running "{thisFilePath}".""")
    logging.info(f"""All other paths will be reported in debugging relative to `projectDir`: "{projectDir}".""")
    logging.info(f"""Script arguments:

    # Arguments
    `DATA_FILE_PATH`: "{DATA_FILE_PATH}"

    # Arguments: General
    `PROJECT_DIR_DEPTH`: "{PROJECT_DIR_DEPTH}"
    `IRB_DIR_DEPTH`: "{IRB_DIR_DEPTH}"
    `IDR_DATA_REQUEST_DIR_DEPTH`: "{IDR_DATA_REQUEST_DIR_DEPTH}"

    `LOG_LEVEL` = "{LOG_LEVEL}"
    """)

    # Script
    _ = pd

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
    timeColumns = ["creationDate",
                   "startDate",
                   "endDate"]
    TABLES_TO_PROCESS = {"Systolic BP": dfSBP,
                         "Diastolic BP": dfDBP}
    for tableName, table in TABLES_TO_PROCESS.items():
        for column in timeColumns:
            table[column] = pd.to_datetime(table[column])

    # Identify groups by medication start and end times.
    # NOTE: Start times are inclusive. End times are not inclusive.
    # I.e., if a medication is between both a stop and start time, it should
    # be assigned to the start time's group.
    AMLODAPINE_START_DATETIME = pd.to_datetime("2023-04-26 15:04:00-04:00")
    AMLODAPINE_STOP_DATETIME = pd.to_datetime("2023-06-11 02:28:00-04:00")
    LOSARTAN_START_DATETIME = pd.to_datetime("2023-06-11 02:28:00-04:00")
    LOSARTAN_STOP_DATETIME = pd.to_datetime(dt.now())
    MEDICATION_DATETIMES = {"Amlodapine Potassium": {"start": AMLODAPINE_START_DATETIME,
                                                     "stop": AMLODAPINE_STOP_DATETIME},
                            "Losartan Potassium": {"start": LOSARTAN_START_DATETIME,
                                                   "stop": LOSARTAN_STOP_DATETIME}}

    for tableName, table in TABLES_TO_PROCESS.items():
        for medication, datetimeDict in MEDICATION_DATETIMES.items():
            startDatetime = datetimeDict["start"]
            stopDatetime = datetimeDict["stop"]
            t0ordinal = startDatetime.toordinal()
            t1ordinal = stopDatetime.toordinal()
            startDateOrdinal = table["startDate"].apply(lambda ts: ts.toordinal())
            endDateOrdinal = table["endDate"].apply(lambda ts: ts.toordinal())
            flagStartDatetime = (t0ordinal <= startDateOrdinal) & (startDateOrdinal <= t1ordinal)
            flagEndDatetime = (t0ordinal < endDateOrdinal) & (endDateOrdinal < t1ordinal)
            flagMedication = flagStartDatetime & flagEndDatetime
            table[medication] = flagMedication
        allMedications = [group for group in MEDICATION_DATETIMES.keys()]
        table["QA: Unassigned (Medications)"] = ~table[allMedications].any()
        logging.info(f"""All observations should be assigned to a group. The current table has {table["QA: Unassigned (Medications)"].sum()} unassigned observations.""")

    # Identify subgroups by time of day
    GROUP_1_START_TIME = pd.to_datetime("03:00:00-04:00")
    GROUP_1_STOP_TIME = pd.to_datetime("12:00:00-04:00")
    GROUP_2_START_TIME = pd.to_datetime("12:00:00-04:00")
    GROUP_2_STOP_TIME = pd.to_datetime("03:00:00-04:00")
    GROUP_TIMES = {"Group 1 (Morning)": {"start": GROUP_1_START_TIME,
                                         "stop": GROUP_1_STOP_TIME},
                   "Group 2 (Evening)": {"start": GROUP_2_START_TIME,
                                         "stop": GROUP_2_STOP_TIME}}

    def time2ordinal(pyTimeObj: datetime.time) -> int:
        """
        Converts a python `datetime.time`-type object into a microseconds-based integer ordinal.
        """
        hours = pyTimeObj.hour
        minutes = pyTimeObj.minute + hours * 60
        seconds = pyTimeObj.second + minutes * 60
        microseconds = pyTimeObj.microsecond + seconds * 10**6
        return microseconds

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
        logging.info(f"""All observations should be assigned to a group. The current table has {table["QA: Unassigned (Groups)"].sum()} unassigned observations.""")

    # Perform statistical tests
    logging.info("""Performing statistical tests.""")
    # Pre-processing
    tablesProcessed = {}
    for tableName, table in TABLES_TO_PROCESS.items():
        tablesProcessed[tableName] = table

    # Define test groups
    COLUMNS_TO_USE_DICT = {"Medications": allMedications,
                           "Time Groups": allGroups,
                           "Medications and Time Groups": allMedications + allGroups}
    modelResults = {tableName: {testGroup: {} for testGroup in COLUMNS_TO_USE_DICT.keys()} for tableName in tablesProcessed.keys()}
    for tableName, table in tablesProcessed.items():
        logging.info(f"""  Working on table "{tableName}".""")
        for testGroup, columnsToUse in COLUMNS_TO_USE_DICT.items():
            logging.info(f"""    Working on test group "{testGroup}".""")
            mask = table[columnsToUse].any(axis=1)
            xx = table[columnsToUse][mask]
            yy = table["value"][mask]
            xtrain = xx.astype(int).to_numpy()
            ytrain = yy.astype(int).to_numpy().reshape(-1, 1)
            # Pre-processing
            xscaler = StandardScaler()
            xscaler.fit(xtrain)
            xstd = xscaler.transform(xtrain)
            yscaler = StandardScaler()
            yscaler.fit(ytrain)
            ystd = yscaler.transform(ytrain)
            # Model training: SKLearn Linear Regression
            model1 = LinearRegression(fit_intercept=True)
            model1 = model1.fit(X=xstd,
                                y=ystd)
            # Model training: StatsModel Linear Regression
            exog = sm.add_constant(xx.astype(float))
            endog = yy.astype(int)
            model2 = sm.OLS(exog=exog, endog=endog)
            result = model2.fit()
            logging.info("""    Model training complete.""")
            # Save model results
            modelResults[tableName][testGroup]["sklearn"] = model1
            modelResults[tableName][testGroup]["sm"] = model2

    # Print results for SKLearn Linear Regression
    logging.info("""StatsModel model results""")
    for tableName, table in tablesProcessed.items():
        logging.info(f"""  Table "{tableName}".""")
        for testGroup, columnsToUse in COLUMNS_TO_USE_DICT.items():
            logging.info(f"""  Test group "{testGroup}".""")
            model = modelResults[tableName][testGroup]["sklearn"]
            logging.info(f"""  ..  Model coefficients: {model.coef_}.""")

    # Print results for StatsModel Linear Regression
    logging.info("""StatsModel model results""")
    for tableName, table in tablesProcessed.items():
        logging.info(f"""  Table "{tableName}".""")
        for testGroup, columnsToUse in COLUMNS_TO_USE_DICT.items():
            logging.info(f"""  Test group "{testGroup}".""")
            model = modelResults[tableName][testGroup]["sm"]
            logging.info(f"""  ..  Results summary:\n{model.fit().summary().as_text()}.""")

    # TODO: Conclusion, interpret results
    pass

    # End script
    logging.info(f"""Finished running "{thisFilePath.relative_to(projectDir)}".""")
