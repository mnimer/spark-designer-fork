<!-- Author: Mike Nimer <mikenimer@google.com> -->
# Generate Production PySpark & SDP Code

Synthesize your `.spark` pipeline graph into a standalone Apache Spark 4.x repository from the bottom **Source Code** tab.

### Generation Capabilities

- **Dynamic Gemini Models**: Dynamic discovery queries the Vertex AI global endpoint for active Gemini models. Selection defaults to Flash-class `gemini-4-hydrogen`.
- **Optional Reviewer Pass**: When you enable **AI Code Reviewer Pass**, Gemini runs read-only checks and repairs blocking defects.
- **Imperative PySpark Output**: Imperative mode generates `src/pipeline.py` with `read_sources`, pure `transform`, and `write_sinks`. It also writes `pytest` unit tests in `tests/test_pipeline.py`.
- **Declarative SDP Output**: SDP mode generates `spark-pipeline.yml` and dataset definitions in `transformations/*.py`. Pure helpers live in `src/transforms.py` with unit tests in `tests/test_transforms.py`.
- **Security & Ownership**: The builder redacts credentials before prompt assembly and tracks generated file ownership in `.sparkgen.json`.

### Generate & Deploy Your Repository

1. Open the bottom **Source Code** tab in the pipeline editor.
2. Choose a discovered Gemini model in **Codegen Model** (`gemini-4-hydrogen` default).
3. Select a workspace **Target Folder** for the generated repository.
4. If you want automated defect repair, toggle **AI Code Reviewer Pass** on.
5. Click **Generate Code** to write source files, `requirements.txt`, `Dockerfile`, and launch scripts.
6. Open the **Deployment** tab to run on Local Spark, Dataproc Clusters, or Serverless Spark.
