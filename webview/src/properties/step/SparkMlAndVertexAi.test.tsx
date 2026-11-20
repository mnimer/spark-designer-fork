/**
 * @fileoverview Unit tests for the SparkMlAndVertexAi step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SparkMlAndVertexAiProperties } from './SparkMlAndVertexAi';

describe('SparkMlAndVertexAiProperties', () => {
  const createNode = (
    properties: Record<string, unknown> = {},
    sparkTransform = 'SparkMlAndVertexAi'
  ) => ({
    id: 'step-ml-1',
    data: {
      label: 'Spark ML & Vertex AI',
      spark_transform: sparkTransform,
      properties,
    },
  });

  it('defaults to spark_ml with features, prediction, and convertVectorsToArray enabled', () => {
    const handleChange = vi.fn();
    render(<SparkMlAndVertexAiProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByLabelText(/Inference Engine/i, { selector: 'select' })).toHaveValue(
      'spark_ml'
    );
    expect(screen.getByLabelText(/Features Column/i, { selector: 'input' })).toHaveValue(
      'features'
    );
    expect(screen.getByLabelText(/Prediction Column/i, { selector: 'input' })).toHaveValue(
      'prediction'
    );
    expect(screen.getByLabelText(/Convert VectorUDT to Array/i, { selector: 'input' })).toBeChecked();
    expect(screen.getByText('Model artifact URI is required.')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Model Artifact URI/i, { selector: 'input' }), {
      target: { value: 'gs://ml-bucket/models/churn-v1' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        engine: 'spark_ml',
        useXgboost: false,
        useVertexAi: false,
        modelUri: 'gs://ml-bucket/models/churn-v1',
      })
    );
  });

  it('configures xgboost mode and sets useXgboost true for versionMatrix resolution', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <SparkMlAndVertexAiProperties node={createNode()} onChange={handleChange} />
    );

    fireEvent.change(screen.getByLabelText(/Inference Engine/i, { selector: 'select' }), {
      target: { value: 'xgboost' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        engine: 'xgboost',
        useXgboost: true,
        useVertexAi: false,
        xgboostTask: 'classification',
      })
    );

    rerender(
      <SparkMlAndVertexAiProperties
        node={createNode({
          engine: 'xgboost',
          modelUri: 'gs://ml-bucket/xgb-model',
          xgboostTask: 'classification',
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/XGBoost Task/i, { selector: 'select' }), {
      target: { value: 'regression' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        engine: 'xgboost',
        useXgboost: true,
        xgboostTask: 'regression',
      })
    );
  });

  it('defaults VertexAiInference alias to vertex_ai_endpoint with us-central1 and batchSize 32', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <SparkMlAndVertexAiProperties
        node={createNode({}, 'VertexAiInference')}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText(/Inference Engine/i, { selector: 'select' })).toHaveValue(
      'vertex_ai_endpoint'
    );
    expect(screen.getByLabelText(/Vertex AI Location/i, { selector: 'input' })).toHaveValue(
      'us-central1'
    );
    expect(screen.getByLabelText(/Prediction Batch Size/i, { selector: 'input' })).toHaveValue(32);
    expect(screen.getByLabelText(/Output Response Column/i, { selector: 'input' })).toHaveValue(
      'ai_response'
    );
    expect(screen.getByText('GCP project ID is required.')).toBeInTheDocument();
    expect(screen.getByText('Vertex AI Endpoint ID is required.')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Input Columns/i, { selector: 'input' }), {
      target: { value: 'amount, merchant_category' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Add Vertex input column' }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        engine: 'vertex_ai_endpoint',
        useVertexAi: true,
        inputColumns: ['amount', 'merchant_category'],
      })
    );

    rerender(
      <SparkMlAndVertexAiProperties
        node={createNode({
          engine: 'vertex_ai_endpoint',
          projectId: 'prod-proj',
          endpointId: '987654321',
          inputColumns: ['amount', 'merchant_category'],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove Vertex input column amount' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        inputColumns: ['merchant_category'],
      })
    );
  });

  it('enforces global location and gemini-4-hydrogen rules in vertex_ai_gemini mode', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <SparkMlAndVertexAiProperties
        node={createNode({ engine: 'vertex_ai_endpoint', location: 'us-central1' })}
        onChange={handleChange}
      />
    );

    // Switching to vertex_ai_gemini resets location to global and model to gemini-4-hydrogen
    fireEvent.change(screen.getByLabelText(/Inference Engine/i, { selector: 'select' }), {
      target: { value: 'vertex_ai_gemini' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        engine: 'vertex_ai_gemini',
        useVertexAi: true,
        useXgboost: false,
        location: 'global',
        geminiModel: 'gemini-4-hydrogen',
      })
    );

    // Regional us-central1 with gemini-4-hydrogen shows 404 validation error
    rerender(
      <SparkMlAndVertexAiProperties
        node={createNode({
          engine: 'vertex_ai_gemini',
          projectId: 'ai-proj',
          location: 'us-central1',
          geminiModel: 'gemini-4-hydrogen',
          promptTemplate: 'Summarize: {body}',
        })}
        onChange={handleChange}
      />
    );
    expect(
      screen.getByText(/Location must be global for Gemini 3\.x\/4\.x models/i)
    ).toBeInTheDocument();

    // Regional us-central1 with gemini-3.8-flash also triggers global location error
    rerender(
      <SparkMlAndVertexAiProperties
        node={createNode({
          engine: 'vertex_ai_gemini',
          projectId: 'ai-proj',
          location: 'us-central1',
          geminiModel: 'gemini-3.8-flash',
          promptTemplate: 'Summarize: {body}',
        })}
        onChange={handleChange}
      />
    );
    expect(
      screen.getByText(/Location must be global for Gemini 3\.x\/4\.x models/i)
    ).toBeInTheDocument();

    // Deprecated gemini-2.5* model shows forbidden validation error
    rerender(
      <SparkMlAndVertexAiProperties
        node={createNode({
          engine: 'vertex_ai_gemini',
          projectId: 'ai-proj',
          location: 'global',
          geminiModel: 'gemini-2.5-flash',
          promptTemplate: 'Summarize: {body}',
        })}
        onChange={handleChange}
      />
    );
    expect(
      screen.getByText(/Legacy Gemini 1.x, 2.0, and 2.5 models are deprecated/i)
    ).toBeInTheDocument();

    // Valid global + gemini-4-hydrogen updates instructions cleanly
    rerender(
      <SparkMlAndVertexAiProperties
        node={createNode({
          engine: 'vertex_ai_gemini',
          projectId: 'ai-proj',
          location: 'global',
          geminiModel: 'gemini-4-hydrogen',
          promptTemplate: 'Summarize: {body}',
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }),
      { target: { value: 'Return JSON sentiment label and confidence' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Return JSON sentiment label and confidence',
      })
    );
  });
});
