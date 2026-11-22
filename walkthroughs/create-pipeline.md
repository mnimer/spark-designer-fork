<!-- Author: Mike Nimer <mikenimer@google.com> -->
# Create Your First Apache Spark Pipeline

The extension saves visual pipeline definitions as human-readable `.spark` YAML files in your workspace.

### Create a New Pipeline

1. Click **Create New Pipeline** below (or **`+`** in **Spark Explorer**).
2. Select **Imperative PySpark (Default)** or **Spark Declarative Pipelines (SDP)**.
3. Save the new pipeline file as `<name>.spark` inside your workspace folder.
4. Connect Sources, Steps, and Sinks on the visual canvas editor.

After you save the file, VS Code opens the `.spark` visual editor automatically.

### Browse Six Starter Templates

Open the **Samples** tab in the left palette to explore six bundled `.spark` starter templates:

- **Batch & Broadcast Joins**: Inspect `gcs-parquet-etl.spark` and `bigquery-cloudsql-broadcast-join.spark`.
- **Structured Streaming**: Inspect `generate-mock-data.spark` and `kafka-streaming-to-iceberg.spark`.
- **Spark Declarative Pipelines**: Inspect `sdp-biglake-iceberg-medallion.spark` and `sdp-auto-cdc-scd1.spark`.

***

[Create New Pipeline](command:spark-pipeline-builder.createNewPipeline)
