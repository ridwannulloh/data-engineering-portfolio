"""
Delta Lake write helpers for LogiStream.

Centralises all writeStream configuration so the main streaming job
stays focused on data transformation logic.
"""

from pyspark.sql import DataFrame


def delta_sink(
    df: DataFrame,
    path: str,
    checkpoint: str,
    output_mode: str = "append",
    trigger_secs: int = 30,
):
    """
    Write a streaming DataFrame to a Delta Lake table.

    Args:
        df:           Streaming DataFrame to sink.
        path:         Delta table destination path.
        checkpoint:   Streaming checkpoint directory (must be unique per query).
        output_mode:  'append' (default) or 'complete' for aggregation queries.
        trigger_secs: Micro-batch trigger interval in seconds.

    Returns:
        StreamingQuery handle.
    """
    return (
        df.writeStream
        .format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(processingTime=f"{trigger_secs} seconds")        .start(path)
    )
