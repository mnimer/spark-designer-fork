/**
 * @fileoverview Unit tests for the TextChunkAndEmbed step property configuration panel (EXT7).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates TextChunkAndEmbedProperties across chunk_and_embed, chunk_only, and embed_only
 * modes, chunkOverlap >= chunkSize and negative overlap validation, deprecated legacy model
 * rejection, batchSize 1..250 bounds, DLQ guidance banner, SystemInstructionsField updates,
 * and credential redaction key safety.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { TextChunkAndEmbedProperties } from './TextChunkAndEmbed';
import { classifyKey } from '../../../../src/credentialRedaction';

describe('TextChunkAndEmbedProperties (EXT7)', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-rag-1',
    data: {
      label: 'Text Chunk & Embed',
      spark_transform: 'TextChunkAndEmbed',
      properties,
    },
  });

  it('defaults to chunk_and_embed with canonical defaults, global location, gemini-embedding-001, and DLQ banner', () => {
    const handleChange = vi.fn();
    render(<TextChunkAndEmbedProperties node={createNode()} onChange={handleChange} />);

    expect(
      screen.getByLabelText(/RAG Preparation Mode/i, { selector: 'select' })
    ).toHaveValue('chunk_and_embed');
    expect(screen.getByLabelText(/Source Text Column/i, { selector: 'input' })).toHaveValue(
      'text'
    );
    expect(screen.getByLabelText(/Document ID Column/i, { selector: 'input' })).toHaveValue(
      'doc_id'
    );
    expect(screen.getByLabelText(/Chunking Strategy/i, { selector: 'select' })).toHaveValue(
      'recursive_character'
    );
    expect(
      screen.getByLabelText(/Chunk Size \(Characters\)/i, { selector: 'input' })
    ).toHaveValue(1000);
    expect(
      screen.getByLabelText(/Chunk Overlap \(Characters\)/i, { selector: 'input' })
    ).toHaveValue(200);
    expect(screen.getByLabelText(/Chunk ID Column/i, { selector: 'input' })).toHaveValue(
      'chunk_id'
    );
    expect(screen.getByLabelText(/Chunk Index Column/i, { selector: 'input' })).toHaveValue(
      'chunk_index'
    );
    expect(screen.getByLabelText(/Chunk Text Column/i, { selector: 'input' })).toHaveValue(
      'chunk_text'
    );

    expect(screen.getByText('GCP project ID is required when embedding.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Vertex AI Location/i, { selector: 'input' })).toHaveValue(
      'global'
    );
    expect(screen.getByLabelText(/Embedding Model/i, { selector: 'input' })).toHaveValue(
      'gemini-embedding-001'
    );
    expect(screen.getByLabelText(/Embedding Task Type/i, { selector: 'select' })).toHaveValue(
      'RETRIEVAL_DOCUMENT'
    );
    expect(screen.getByLabelText(/Output Dimensionality/i, { selector: 'input' })).toHaveValue(
      768
    );
    expect(screen.getByLabelText(/Embedding Batch Size/i, { selector: 'input' })).toHaveValue(64);
    expect(screen.getByLabelText(/Embedding Output Column/i, { selector: 'input' })).toHaveValue(
      'embedding'
    );
    expect(screen.getByTestId('text-chunk-embed-dlq-banner')).toBeInTheDocument();

    // Container-managed cards must never be duplicated inside step panels
    expect(screen.queryByText('Docs ↗')).not.toBeInTheDocument();
  });

  it('hides Vertex AI embedding fields in chunk_only mode and hides chunking fields in embed_only mode', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <TextChunkAndEmbedProperties node={createNode()} onChange={handleChange} />
    );

    fireEvent.change(screen.getByLabelText(/RAG Preparation Mode/i, { selector: 'select' }), {
      target: { value: 'chunk_only' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({ mode: 'chunk_only' })
    );

    rerender(
      <TextChunkAndEmbedProperties
        node={createNode({ mode: 'chunk_only' })}
        onChange={handleChange}
      />
    );
    expect(screen.getByLabelText(/Document ID Column/i, { selector: 'input' })).toBeInTheDocument();
    expect(
      screen.queryByLabelText(/GCP Project ID/i, { selector: 'input' })
    ).not.toBeInTheDocument();
    expect(
      screen.queryByLabelText(/Embedding Model/i, { selector: 'input' })
    ).not.toBeInTheDocument();
    expect(screen.queryAllByRole('alert')).toHaveLength(0);

    rerender(
      <TextChunkAndEmbedProperties
        node={createNode({ mode: 'embed_only', projectId: 'rag-proj' })}
        onChange={handleChange}
      />
    );
    expect(
      screen.queryByLabelText(/Document ID Column/i, { selector: 'input' })
    ).not.toBeInTheDocument();
    expect(
      screen.queryByLabelText(/Chunk Size \(Characters\)/i, { selector: 'input' })
    ).not.toBeInTheDocument();
    expect(screen.getByLabelText(/GCP Project ID/i, { selector: 'input' })).toHaveValue(
      'rag-proj'
    );
    expect(screen.getByLabelText(/Embedding Model/i, { selector: 'input' })).toHaveValue(
      'gemini-embedding-001'
    );
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
  });

  it('rejects chunkOverlap >= chunkSize, negative chunkOverlap, and non-positive chunkSize', () => {
    const { rerender } = render(
      <TextChunkAndEmbedProperties
        node={createNode({
          mode: 'chunk_only',
          chunkSize: 500,
          chunkOverlap: 500,
        })}
        onChange={vi.fn()}
      />
    );

    expect(
      screen.getByText('Chunk overlap must be strictly less than chunk size.')
    ).toBeInTheDocument();

    rerender(
      <TextChunkAndEmbedProperties
        node={createNode({
          mode: 'chunk_only',
          chunkSize: 500,
          chunkOverlap: 650,
        })}
        onChange={vi.fn()}
      />
    );
    expect(
      screen.getByText('Chunk overlap must be strictly less than chunk size.')
    ).toBeInTheDocument();

    rerender(
      <TextChunkAndEmbedProperties
        node={createNode({
          mode: 'chunk_only',
          chunkSize: 0,
          chunkOverlap: -5,
        })}
        onChange={vi.fn()}
      />
    );
    expect(
      screen.getByText('Chunk size must be a positive integer greater than 0.')
    ).toBeInTheDocument();
    expect(
      screen.getByText('Chunk overlap must be a non-negative integer (0 or greater).')
    ).toBeInTheDocument();

    rerender(
      <TextChunkAndEmbedProperties
        node={createNode({
          mode: 'chunk_only',
          chunkSize: 500,
          chunkOverlap: 0,
        })}
        onChange={vi.fn()}
      />
    );
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
  });

  it('rejects deprecated legacy embedding models and validates batchSize 1..250 bounds', () => {
    const { rerender } = render(
      <TextChunkAndEmbedProperties
        node={createNode({
          mode: 'embed_only',
          projectId: 'rag-proj',
          embeddingModel: 'textembedding-gecko@003',
          batchSize: 0,
          outputDimensionality: 0,
        })}
        onChange={vi.fn()}
      />
    );

    expect(
      screen.getByText(
        /Legacy textembedding-gecko, Gemini 1\.x, 2\.0, and 2\.5 models are deprecated/i
      )
    ).toBeInTheDocument();
    expect(
      screen.getByText('Batch size must be an integer between 1 and 250.')
    ).toBeInTheDocument();
    expect(
      screen.getByText('Output dimensionality must be a positive integer.')
    ).toBeInTheDocument();

    for (const legacyModel of ['gemini-1.5-pro', 'gemini-2.0-flash-001', 'gemini-2.5-flash']) {
      rerender(
        <TextChunkAndEmbedProperties
          node={createNode({
            mode: 'embed_only',
            projectId: 'rag-proj',
            embeddingModel: legacyModel,
            batchSize: 251,
          })}
          onChange={vi.fn()}
        />
      );
      expect(
        screen.getByText(
          /Legacy textembedding-gecko, Gemini 1\.x, 2\.0, and 2\.5 models are deprecated/i
        )
      ).toBeInTheDocument();
      expect(
        screen.getByText('Batch size must be an integer between 1 and 250.')
      ).toBeInTheDocument();
    }

    // Valid modern models (gemini-embedding-001 and text-embedding-005) and batchSize=250 pass
    rerender(
      <TextChunkAndEmbedProperties
        node={createNode({
          mode: 'embed_only',
          projectId: 'rag-proj',
          embeddingModel: 'text-embedding-005',
          batchSize: 250,
          outputDimensionality: 768,
        })}
        onChange={vi.fn()}
      />
    );
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
  });

  it('emits typed property updates across chunking, embedding, and system_instructions safely', () => {
    const emitted: Array<Record<string, unknown>> = [];
    const handleChange = vi.fn((next) => emitted.push(next));

    render(
      <TextChunkAndEmbedProperties
        node={createNode({
          mode: 'chunk_and_embed',
          projectId: 'rag-proj',
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Chunking Strategy/i, { selector: 'select' }), {
      target: { name: 'chunkStrategy', value: 'sentence' },
    });
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({ chunkStrategy: 'sentence' })
    );

    fireEvent.change(screen.getByLabelText(/Chunk Size \(Characters\)/i, { selector: 'input' }), {
      target: { name: 'chunkSize', value: '1200' },
    });
    expect(handleChange).toHaveBeenLastCalledWith(expect.objectContaining({ chunkSize: 1200 }));

    fireEvent.change(
      screen.getByLabelText(/Chunk Overlap \(Characters\)/i, { selector: 'input' }),
      {
        target: { name: 'chunkOverlap', value: '150' },
      }
    );
    expect(handleChange).toHaveBeenLastCalledWith(expect.objectContaining({ chunkOverlap: 150 }));

    fireEvent.change(screen.getByLabelText(/Embedding Task Type/i, { selector: 'select' }), {
      target: { name: 'taskType', value: 'RETRIEVAL_QUERY' },
    });
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({ taskType: 'RETRIEVAL_QUERY' })
    );

    fireEvent.change(screen.getByLabelText(/Output Dimensionality/i, { selector: 'input' }), {
      target: { name: 'outputDimensionality', value: '1536' },
    });
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({ outputDimensionality: 1536 })
    );

    fireEvent.change(screen.getByLabelText(/Embedding Batch Size/i, { selector: 'input' }), {
      target: { name: 'batchSize', value: '128' },
    });
    expect(handleChange).toHaveBeenLastCalledWith(expect.objectContaining({ batchSize: 128 }));

    fireEvent.change(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }),
      {
        target: { value: 'Preserve Markdown section headers on each chunk' },
      }
    );
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        system_instructions: 'Preserve Markdown section headers on each chunk',
      })
    );

    for (const payload of emitted) {
      for (const key of Object.keys(payload)) {
        expect(['safe', 'reference']).toContain(classifyKey(key));
      }
    }
  });
});
