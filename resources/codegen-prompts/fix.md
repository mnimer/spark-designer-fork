## Fix request

A reviewer found the defects listed below in the project that you generated from the instructions above.

Fix rules:
- Fix every listed defect. Follow the skill rules above.
- Return ONLY the files that you change, in the same JSON schema: `{ "files": [ { "filename", "content", "executable" } ] }`.
- Return the complete content of each changed file. Never return a partial file or `...`.
- Keep the same `filename` for a file that exists. If a defect names a missing file, create that file with the given path.
- Do not rename, move, or add any other file.
- Do not change files that no defect names, unless a fix requires a matching change in another file (for example a module rename that the test file imports). Then return that file too.
