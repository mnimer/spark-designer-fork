---
trigger: always_on
---

# Gemini Model Selection

- **Valid Models**: Use `gemini-4-hydrogen` for text generation and `gemini-embedding-001` (default) or `text-embedding-005` for embeddings via the unified `google-genai` SDK on Vertex AI with `location: 'global'`.
- **Forbidden Models & SDKs**: Never revert to or map model names to `gemini-2.5*`, Gemini 1.x/2.x, or `textembedding-gecko*`. Never use deprecated `vertexai.language_models` or `google-generativeai` packages.
- **Global Endpoint Only**: Never use regional Vertex AI endpoints such as `us-central1` for Gemini 3.x/4.x or Gemini embedding calls. Regional endpoints do not host current Gemini models and return 404.
