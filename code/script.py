"""
Analysis of blood pressure exported from Apple Health
"""

import json
import logging
from pathlib import Path
# Third-party packages
import pandas as pd
import scipy.stats as sps
import statsmodels.api as sm
from IPython import get_ipython
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
# Local packages
from drapi.drapi import getTimestamp, successiveParents, make_dir_path

# Arguments
DATA_DIRECTORY = Path("data/output/processData/2023-07-10 00-44-33")

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
    `DATA_DIRECTORY`: "{DATA_DIRECTORY}"

    # Arguments: General
    `PROJECT_DIR_DEPTH`: "{PROJECT_DIR_DEPTH}"

    `LOG_LEVEL` = "{LOG_LEVEL}"
    """)

    # Load data directory
    tablesDirectory = DATA_DIRECTORY.joinpath("tablesToProcess")

    # Load tables
    tablesToProcess = {}
    for fpath in tablesDirectory.iterdir():
        table = pd.read_csv(fpath)
        tablesToProcess[fpath.stem] = table

    # Load group and medication lists
    jsonDir = DATA_DIRECTORY.joinpath("jsonDir")

    groupsPath = jsonDir.joinpath("allGroups.JSON")
    with open(groupsPath, "r") as file:
        allGroups = json.loads(file.read())

    medicationsPath = jsonDir.joinpath("allMedications.JSON")
    with open(medicationsPath, "r") as file:
        allMedications = json.loads(file.read())

    # Perform statistical tests
    logging.info("""Performing statistical tests.""")
    # Pre-processing
    tablesProcessed = {}
    for tableName, table in tablesToProcess.items():
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
            logging.info(f"""  ..  Results summary:{model.fit().summary().as_text()}\n\n\n""")

    # TODO Test for difference in variance between groups
    pass

    # t-tests for SBP and DBP
    logging.info("Performing t-tests")
    ttestResults = {tableName: {testGroup: {} for testGroup in COLUMNS_TO_USE_DICT.keys()} for tableName in tablesProcessed.keys()}
    for tableName, table in tablesProcessed.items():
        logging.info(f"""  Working on table "{tableName}".""")
        for testGroup, columnsToUse in COLUMNS_TO_USE_DICT.items():
            logging.info(f"""    Working on test group "{testGroup}".""")
            group0Column = columnsToUse[0]
            for group1Column in columnsToUse[1:]:
                logging.info(f"""  ..  Comparing these two groups: "{group0Column}", "{group1Column}".""")
                group0mask = table[group0Column].values
                group1mask = table[group1Column].values
                group0 = table[group0mask]["value"].astype(float)
                group1 = table[group1mask]["value"].astype(float)
                results = sps.ttest_ind(a=group0,
                                        b=group1)
                ttestResults[tableName][testGroup] = results
                logging.info(f"""  ..    Results - p-value: {round(results.pvalue, 4)}""")
                logging.info(f"""  ..    Results - t-statistics: {round(results.statistic, 4)}""")
                group0Column = group1Column
    # NOTE Conclusion. For both SBP and DBP morning and evening measurements are significantly different (p<0.01).

    # TODO: p-tests for SBP and DBP
    logging.info("Performing p-tests")

    # TODO: Visualize the association between systolic and diastolic measurements.
    pass

    # TODO: Conclusion, interpret results
    # NOTE The medication model has the lowest AIC, suggesting that the blood pressure medications are the best predictors of BP, and not the time-of-day models or the combined medication and time models.

    # End script
    logging.info(f"""Finished running "{thisFilePath.relative_to(projectDir)}".""")
