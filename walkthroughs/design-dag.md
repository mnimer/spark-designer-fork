<!-- Author: Mike Nimer <mikenimer@google.com> -->
# Design with Sources, Transforms & Sinks

Design `.spark` pipelines visually by connecting catalog nodes on the interactive canvas.

### Visual Catalog

- **Sources (40 connectors)**: Read data from BigQuery, BigLake Iceberg, Kafka, Pub/Sub, Cloud SQL, Spanner, and Cloud Storage formats.
- **Steps (33 transforms)**: Apply Filter, WithColumns, Watermark, GroupBy & Aggregate, Join, Merge CDC/SCD, Conditional Split, Spark SQL, and Spark ML & Vertex AI.
- **Sinks (38 destinations)**: Write outputs to BigLake Iceberg, Delta Lake, BigQuery, Kafka, relational databases, and cloud object storage.

### Canvas Badges & Handles

- **Direct Replication**: Connect a `SOURCE -> SINK` edge directly for zero-transform data replication.
- **Execution Badges**: Source and Sink cards show `BATCH` or `STREAM` badges for their active execution mode.
- **Join & Broadcast**: `Join` steps expose `L` and `R` handles. When you enable right-side lookup broadcast, the card displays `BROADCAST`.
- **Dynamic Split Handles**: `ConditionalSplit` steps render output handles for each named rule plus an `else` fallback handle.
- **Dead-Letter Routing**: Error-capable steps expose a red `error` handle to route failed rows to dead-letter sinks.

### Configure Node Properties

1. Drag a Source, Step, or Sink from the left palette onto the canvas.
2. Click any node card on the canvas to open the right-hand **Properties Panel**.
3. Complete the highlighted required fields inside the **Properties Panel**.
4. Select live cloud resources from the `gcloud` and `bq` asset dropdowns.
5. Hover over any property icon to read Apache Spark 4.x tooltips.
