---
trigger: always_on
---

# Gemini Model Selection

- **Valid Model**: Use `gemini-3.8-flash` on Vertex AI with `location: 'global'`.
- **Forbidden Models**: Never revert to or map model names to `gemini-2.5*` or Gemini 1.x/2.x.
- **Global Endpoint Only**: Never use regional Vertex AI endpoints such as `us-central1`. Regional endpoints do not host the current Gemini Flash models and return 404.
