# Generate Production PySpark & SDP Code

Transform your `.spark` visual graph into a complete Apache Spark 4.x repository with Gemini AI.

### Features

- **Dual Paradigm Output**: Synthesize Imperative PySpark (`src/pipeline.py` with `read_sources`, `transform`, and `write_sinks`) or Spark Declarative Pipelines (`spark-pipeline.yml` and `transformations/*.py`).
- **Offline Testable Helpers**: Generates pure DataFrame helpers and `pytest` unit test suites (`tests/test_pipeline.py` or `tests/test_transforms.py`).
- **Ready-to-Run Packaging**: Emits `requirements.txt`, container definitions, and launch scripts for Local Spark, Dataproc Clusters, Serverless Spark, and Managed Data Pipelines.
