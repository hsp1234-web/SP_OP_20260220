import polars as pl
from datetime import timedelta

def align_session(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aligns TAIFEX night session data (T-1 15:00+) to the T session.
    Adds a 'session' column ('Regular', 'AfterHours').

    Note: Calculating the exact 'Trade Date' requires a holiday calendar.
    This function primarily identifies the session type.
    """
    if "timestamp" not in df.columns:
        # Try to construct timestamp if date/Time exist
        if "date" in df.columns and "Time" in df.columns:
             df = df.with_columns(
                 (pl.col("date").dt.strftime("%Y-%m-%d") + " " + pl.col("Time")).str.strptime(pl.Datetime(time_unit="ns"), "%Y-%m-%d %H:%M:%S%.f")
                 .alias("timestamp")
             )
        else:
            return df

    # Define session cutoff (15:00:00)
    # Using timestamp, extract hour
    df = df.with_columns(
        pl.when(pl.col("timestamp").dt.hour() >= 15)
        .then(pl.lit("AfterHours"))
        .otherwise(pl.lit("Regular"))
        .alias("session")
    )

    return df
