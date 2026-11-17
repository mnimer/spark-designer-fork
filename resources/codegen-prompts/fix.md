## Fix request

A senior PySpark 4.x reviewer found the defects listed below in the project that you generated from the instructions, `.spark` pipeline YAML, and `## Execution Plan` above.

Fix rules:
- Fix every listed defect. Follow the skill rules and `## Execution Plan` above.
- Preserve the mandatory 3-phase separation in `src/pipeline.py` (`read_sources(spark, config)`, pure `transform(dfs, config)`, `write_sinks(dfs, config)`) and offline in-memory DataFrame tests in `tests/test_pipeline.py`.
- Return ONLY the files that you change, in the same JSON schema: `{ "files": [ { "filename", "content", "executable" } ] }`.
- Return the complete content of each changed file. Never return a partial file, `TODO`, or `...`.
- Keep the same `filename` for a file that exists. If a defect names a missing file, create that file with the given path.
- Do not rename, move, or add any other file.
- Do not change files that no defect names, unless a fix requires a matching change in another file (for example updating a function signature or helper in `src/pipeline.py` that `tests/test_pipeline.py` imports). Then return that file too.
