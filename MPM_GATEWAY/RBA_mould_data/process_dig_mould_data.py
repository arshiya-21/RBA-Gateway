import json
import pandas as pd


def etl(json_data, last_timepour,mould_config):
    """
    Extracts structured data from the 'moulds' JSON field and converts it to a DataFrame.

    Parameters:
        json_data (dict): JSON data containing the 'moulds' list.
        last_id (int, optional): The last processed MouldIndex.

    Returns:
        pd.DataFrame: Extracted table data.
    """
    if "moulds" not in json_data:
        raise KeyError("The key 'moulds' is missing from the JSON data.")

    if "patterns" not in json_data:
        raise KeyError("The key 'Patterns' is missing from the JSON data.")

    PATTERN_LOOKUP = {k: v[0] for k, v in mould_config["PATTERN_LOOKUP"].items()}
    time_columns = mould_config["TIME_COLUMNS"]
    keep_columns = mould_config["keep_columns"]

    df = pd.DataFrame(json_data["moulds"])

    mask = df["PatternNumber"].astype(str).isin(PATTERN_LOOKUP)
    df.loc[mask, "PatternIdentification"] = df.loc[mask, "PatternNumber"].astype(str).map(PATTERN_LOOKUP)

    for col in time_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace("Z", "", regex=False)  # Remove 'Z' timezone
            df[col] = pd.to_datetime(df[col], errors="coerce", format="%Y-%m-%dT%H:%M:%S.%f")  # Explicit format

    if "SortingStatus" in df.columns:
        df["SortingStatus"] = pd.to_numeric(df["SortingStatus"], errors="coerce").fillna(0.0)

    if "PourStatus" in df.columns:
        df["PourStatus"] = pd.to_numeric(df["PourStatus"], errors="coerce").fillna(0).astype(int)

    # # Keep only columns up to 'BatchID'
    # keep_columns = [
    #     "MouldIndex", "MouldCounter", "SettingIndex", "MouldThickness",
    #     "Compressibility", "CycleTime", "TimeProd", "TimePour", "TimeProdPLC",
    #     "TimeProdDiff", "TimeShakeOut", "MouldStatus", "CoreStatus",
    #     "PourStatus", "SortingStatus", "PatternNumber", "PatternIdentification","MinCycletime",
    #     "PPThickness", "PPHeight", "SPThickness", "SPHeight", "MinimumDist",
    #     "CoreMaskTh", "CoreHeightOut", "CoreHeightIn", "CorrMoldTh",
    #     "CorrMoldPos", "CorrOp3aEnd", "ShotPressure", "ShotTimeCorr",
    #     "SandLevel", "SandValvAct", "SqueezePres", "ExtSqTime", "MinCompress",
    #     "MaxCompress", "DelayedSPSq", "SqueezeSpeed", "Op2A", "Op3A",
    #     "PPStripAcc", "PPStripDist", "SPStripAcc", "SPStripDist", "PPStripAir",
    #     "SPStripAir", "CloseUpForce", "CorrClUpPos", "ExtMoldRetTime",
    #     "MoldRetPres", "CluseUpDec", "CorrMoldDelPos", "AccMoldtransport",
    #     "DecMoldtransport", "CoreSetMode", "CoreRetSel", "VacWithEmptyCM",
    #     "VacWithFullCM", "CSEAccS1S3", "CSECoreSettCorr", "CSECoreSetForce",
    #     "CSECoreSetTime", "CSECoreStripAir", "CSECoreStripTime",
    #     "CSEStripAccS6", "CSEStripDistS6", "SprayFreq", "SprayTime",
    #     "SPSprayPos", "ActNozzles", "AtomizingAirPress", "PPTemp", "SPTemp",
    #     "BlowSPTopOp3", "BlowOffFront4A", "BlowPPImprCSE", "OverlapOp3B4A",
    #     "OverlapOp56", "OverlapOp61", "SpeedOnCams3_6", "OverlapOp36CSE",
    #     "OverlapCSEOp1223", "OverlapCSEOp6778", "CIM4ProdIndex",
    #     "CIM4UptimeIndex", "BatchID"
    # ]

    # Retain only the required columns
    df = df[keep_columns]

    # Filter only records where TimePour is **not NULL**
    df = df[df["TimePour"].notna()]

    # Ensure last_timepour is considered if provided
    if last_timepour:
        df = df[df["TimePour"] > pd.to_datetime(last_timepour, errors="coerce")]

    # Sort values by TimePour
    df.sort_values("TimePour", inplace=True)


    return df
