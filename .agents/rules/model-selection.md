---
trigger: always_on
---

# Gemini Model Selection

- **Valid Models**:
  - **Dynamic Code Generation (`src/codegen/modelDiscovery.ts`)**: Discover models dynamically via `@google/genai` `ai.models.list({ config: { queryBase: true } })` across Vertex AI (`location: 'global'`) and the Gemini Developer API. Filter out deprecated `gemini-1.*`/`gemini-2.*`, embedding, media, tuned, and live-only models. Rank Gemini 3.x+ and 4.x text generation models by descending version. Default to the latest Flash-class model, such as `gemini-4-hydrogen` over `gemini-3.8-flash`. If discovery fails or lacks credentials, fall back to `DEFAULT_GEMINI_MODEL = 'gemini-4-hydrogen'`.
  - **Generated PySpark Runtime Code & Evaluators**: Generated PySpark steps such as `SparkMlAndVertexAi` and evaluator agents default to `gemini-4-hydrogen` via `google-genai` on Vertex AI `location: 'global'`. Embedding steps such as `TextChunkAndEmbed` default to `gemini-embedding-001` (or `text-embedding-005`) on Vertex AI `location: 'global'`.
- **Forbidden Models & SDKs**: Never revert to or map model names to `gemini-2.5*`, Gemini 1.x/2.x, or `textembedding-gecko*`. Never use deprecated `vertexai.language_models` or `google-generativeai` packages.
- **Global Endpoint Only**: Never use regional Vertex AI endpoints such as `us-central1` for Gemini 3.x/4.x or Gemini embedding calls. Regional endpoints do not host current Gemini models and return 404.
