<!-- Author: Mike Nimer <mikenimer@google.com> -->
# Build Spark Declarative Pipelines (Spark 4.2+)

Design incremental medallion tables and SCD Type 1 Auto CDC flows with **Spark Declarative Pipelines (SDP)** on Apache Spark 4.2+.

### SDP Canvas & Dataset Badges

- **Mode Selection**: When you create a `.spark` file, select **Spark Declarative Pipelines (SDP)** or use **`SDP (4.2+)`** in the toolbar.
- **Dataset Badges**: Canvas nodes display deterministic `VIEW`, `MV`, `TABLE`, `CDC`, and `SINK` dataset badges.
- **Compatibility Guard**: Unsupported `foreachBatch` sinks, Spark ML steps, and SCD Type 2 options display explanatory palette tooltips.

### Configure & Run SDP Pipelines

1. Click the **`SDP (4.2+)`** toolbar switcher to enable SDP mode on an active `.spark` pipeline.
2. Open the **Deployment** tab to configure pipeline storage and catalog settings.
3. Set an absolute `gs://` checkpoint `storage` URI outside source folders.
4. Enter a plain `catalog` alias (`lake`), `database`, and BigLake Iceberg REST warehouse URI.
5. Run `dry-run` to validate your SDP graph without writing data.
6. Execute incremental `run`, selective `--refresh`, or confirmed `--full-refresh` updates.

### Explore Bundled SDP Samples

Open the **Samples** tab in the left palette to inspect **`sdp-biglake-iceberg-medallion.spark`** and **`sdp-auto-cdc-scd1.spark`**.
