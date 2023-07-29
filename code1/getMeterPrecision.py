"""
Calculate the precision of the Beur blood pressure meter
"""

import logging
from pathlib import Path
# Third-party packages
from IPython import get_ipython
import pandas as pd
# Local packages
from drapi.drapi import getTimestamp, successiveParents, make_dir_path
from appleHealthExport.code.functions import parseExportFile, getRecordTypes, getRecordsByAttributeValue, tabulateRecords

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
    timeColumns = ["creationDate",
                   "startDate",
                   "endDate"]
    TABLES_TO_PROCESS = {"Systolic BP": dfSBP,
                         "Diastolic BP": dfDBP}
    for tableName, table in TABLES_TO_PROCESS.items():
        for column in timeColumns:
            table[column] = pd.to_datetime(table[column])
