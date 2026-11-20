# Build Spark Declarative Pipelines (Spark 4.2+)

Design incremental medallion tables and SCD Type 1 Auto CDC flows using **Spark Declarative Pipelines (SDP)** targeting Apache Spark 4.2+.

### How SDP Mode Works

1. **Switch or Create in SDP Mode**: Pick **Spark Declarative Pipelines (SDP)** when creating a `.spark` file, or click the **`SDP (4.2+)`** switcher in the canvas toolbar.
2. **Inspect Dataset Badges**: Nodes display deterministic SDP badges (`VIEW`, `MV`, `TABLE`, `CDC`, and `SINK`). Unsupported nodes (`foreachBatch` sinks, Spark ML, and SCD Type 2) show explanatory tooltips in the palette.
3. **Configure BigLake Iceberg & Storage**: In the **Deployment** tab, set an absolute `gs://` checkpoint `storage` URI outside source folders, a plain `catalog` alias (`lake`), `database`, and BigLake REST warehouse URI.
4. **Validate & Refresh Safely**: Run `dry-run` to validate your graph without writing data, or choose incremental `run`, `--refresh`, or confirmed `--full-refresh` updates.

### Explore Bundled SDP Samples

Open the **Samples** tab in the left palette to inspect **`sdp-biglake-iceberg-medallion.spark`** and **`sdp-auto-cdc-scd1.spark`**.
