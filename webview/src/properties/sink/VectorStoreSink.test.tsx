/**
 * @fileoverview Unit Tests for VectorStoreSinkProperties Component (EXT3).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates VectorStoreSinkProperties across alloydb_pgvector and vertex_vector_search
 * backends, writeMode toggles, required and numeric validation, SQL identifier checks,
 * Secret Manager authentication, CollapsibleSection JAR overrides, StoragePathField GCS
 * export URI validation, system_instructions updates, and credential redaction key safety.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { VectorStoreSinkProperties } from './VectorStoreSink';
import { classifyKey } from '../../../../src/credentialRedaction';

interface VsCodeWindow {
  vscode?: {
    postMessage: ReturnType<typeof vi.fn>;
  };
}

describe('VectorStoreSinkProperties (EXT3)', () => {
  let postMessageSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.clearAllMocks();
    postMessageSpy = vi.fn();
    (window as unknown as VsCodeWindow).vscode = {
      postMessage: postMessageSpy,
    };
  });

  afterEach(() => {
    delete (window as unknown as VsCodeWindow).vscode;
  });

  it('defaults to alloydb_pgvector with Conscrypt note, vector defaults, and required field errors', () => {
    render(<VectorStoreSinkProperties node={{ data: { properties: {} } }} onChange={vi.fn()} />);

    expect(
      screen.getByLabelText(/Vector Store Backend/i, { selector: 'select' })
    ).toHaveValue('alloydb_pgvector');
    expect(screen.getByText(/dataproc\.artifacts\.remove=conscrypt/i)).toBeInTheDocument();

    expect(screen.getByText('AlloyDB instance URI is required.')).toBeInTheDocument();
    expect(screen.getByText('Database is required.')).toBeInTheDocument();
    expect(screen.getByText('Table name is required.')).toBeInTheDocument();
    expect(screen.getByText('Database user is required.')).toBeInTheDocument();

    expect(screen.getByLabelText(/Schema Name/i, { selector: 'input' })).toHaveValue('public');
    expect(
      screen.getByLabelText(/ID Column \(Primary Key\)/i, { selector: 'input' })
    ).toHaveValue('chunk_id');
    expect(screen.getByLabelText(/Embedding Column/i, { selector: 'input' })).toHaveValue(
      'embedding'
    );
    expect(screen.getByLabelText(/Content Column/i, { selector: 'input' })).toHaveValue(
      'chunk_text'
    );
    expect(screen.getByLabelText(/Vector Dimensions/i, { selector: 'input' })).toHaveValue(768);
    expect(screen.getByLabelText(/Upsert Batch Size/i, { selector: 'input' })).toHaveValue(500);
    expect(
      screen.getByLabelText(/Upsert Conflict Action/i, { selector: 'select' })
    ).toHaveValue('do_update');

    // Container-managed cards must never be duplicated inside individual sink forms
    expect(screen.queryByText('Streaming Sink Settings')).not.toBeInTheDocument();
    expect(screen.queryByText('Docs ↗')).not.toBeInTheDocument();
  });

  it('validates AlloyDB URI format, SQL identifiers, and positive numeric bounds', () => {
    const { rerender } = render(
      <VectorStoreSinkProperties
        node={{
          data: {
            properties: {
              backend: 'alloydb_pgvector',
              alloydbInstanceUri: 'not-a-valid-uri',
              database: 'bad/db',
              schemaName: 'public; DROP TABLE x',
              table: 'bad-table-name',
              dbUser: 'postgres',
              idColumn: '123_bad',
              embeddingColumn: 'bad col',
              contentColumn: 'bad$col',
              metadataColumns: 'doc_id, invalid-col!',
              vectorDimensions: 0,
              batchSize: -10,
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(
      screen.getByText(
        'Use projects/<project>/locations/<region>/clusters/<cluster>/instances/<instance>.'
      )
    ).toBeInTheDocument();
    expect(
      screen.getByText('Database name cannot contain spaces or / ? # @ :')
    ).toBeInTheDocument();
    expect(
      screen.getByText('Schema name must be a valid SQL identifier (letters, digits, underscores).')
    ).toBeInTheDocument();
    expect(
      screen.getByText('Table name must be a valid SQL identifier (letters, digits, underscores).')
    ).toBeInTheDocument();
    expect(screen.getByText('ID column must be a valid SQL identifier.')).toBeInTheDocument();
    expect(
      screen.getByText('Embedding column must be a valid SQL identifier.')
    ).toBeInTheDocument();
    expect(screen.getByText('Content column must be a valid SQL identifier.')).toBeInTheDocument();
    expect(screen.getByText('Invalid SQL column identifier: invalid-col!')).toBeInTheDocument();
    expect(
      screen.getByText('Vector dimensions must be a positive integer greater than 0.')
    ).toBeInTheDocument();
    expect(
      screen.getByText('Batch size must be a positive integer greater than 0.')
    ).toBeInTheDocument();

    rerender(
      <VectorStoreSinkProperties
        node={{
          data: {
            properties: {
              backend: 'alloydb_pgvector',
              alloydbInstanceUri:
                'projects/rag-proj/locations/us-central1/clusters/rag-c1/instances/primary',
              ipType: 'PSC',
              database: 'ragdb',
              schemaName: 'vectors',
              table: 'doc_embeddings',
              dbUser: 'rag_writer',
              idColumn: 'chunk_id',
              embeddingColumn: 'embedding',
              contentColumn: 'chunk_text',
              metadataColumns: ['doc_id', 'source_uri'],
              vectorDimensions: 1536,
              upsertConflictAction: 'do_nothing',
              batchSize: 250,
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.queryAllByRole('alert')).toHaveLength(0);
    expect(screen.getByLabelText(/Metadata Columns/i, { selector: 'input' })).toHaveValue(
      'doc_id, source_uri'
    );
  });

  it('handles Secret Manager authentication, CollapsibleSection JAR fields, and system_instructions on AlloyDB', () => {
    const emitted: Array<Record<string, unknown>> = [];
    const handleChange = vi.fn((next) => emitted.push(next));

    const { rerender } = render(
      <VectorStoreSinkProperties
        node={{
          data: {
            properties: {
              backend: 'alloydb_pgvector',
              alloydbInstanceUri:
                'projects/rag-proj/locations/us-central1/clusters/c1/instances/i1',
              database: 'postgres',
              table: 'embeddings',
              dbUser: 'postgres',
              authMode: 'secret_manager',
              secretName: '',
            },
          },
        }}
        onChange={handleChange}
      />
    );

    expect(postMessageSpy).toHaveBeenCalledWith({ command: 'getSecrets' });
    expect(
      screen.getByText(
        'Secret Manager secret name is required when password secret authentication is selected.'
      )
    ).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Secret Manager Secret/i, { selector: 'input' }), {
      target: { name: 'secretName', value: 'alloydb-rag-password' },
    });
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        backend: 'alloydb_pgvector',
        secretName: 'alloydb-rag-password',
      })
    );

    fireEvent.change(screen.getByLabelText(/Vector Dimensions/i, { selector: 'input' }), {
      target: { name: 'vectorDimensions', value: '3072' },
    });
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        vectorDimensions: 3072,
      })
    );

    // Expand CollapsibleSection for optional JAR URIs
    const jarToggle = screen.getByRole('button', {
      name: /Optional AlloyDB JVM Socket Factory JARs/i,
    });
    expect(jarToggle).toHaveAttribute('aria-expanded', 'false');
    fireEvent.click(jarToggle);
    expect(jarToggle).toHaveAttribute('aria-expanded', 'true');

    fireEvent.change(
      screen.getByLabelText(/AlloyDB Socket Factory JAR URI/i, { selector: 'input' }),
      {
        target: {
          name: 'socketFactoryJarUri',
          value: 'gs://rag-jars/alloydb-jdbc-connector-1.2.0.jar',
        },
      }
    );
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        socketFactoryJarUri: 'gs://rag-jars/alloydb-jdbc-connector-1.2.0.jar',
      })
    );

    fireEvent.change(screen.getByLabelText(/PostgreSQL Driver JAR URI/i, { selector: 'input' }), {
      target: {
        name: 'driverJarUri',
        value: 'gs://rag-jars/postgresql-42.7.4.jar',
      },
    });
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        driverJarUri: 'gs://rag-jars/postgresql-42.7.4.jar',
      })
    );

    rerender(
      <VectorStoreSinkProperties
        node={{
          data: {
            properties: {
              backend: 'alloydb_pgvector',
              alloydbInstanceUri:
                'projects/rag-proj/locations/us-central1/clusters/c1/instances/i1',
              database: 'postgres',
              table: 'embeddings',
              dbUser: 'postgres',
            },
          },
        }}
        onChange={handleChange}
      />
    );

    fireEvent.change(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }),
      {
        target: { value: 'Cast metadata json columns explicitly' },
      }
    );
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        system_instructions: 'Cast metadata json columns explicitly',
      })
    );

    for (const payload of emitted) {
      for (const key of Object.keys(payload)) {
        expect(['safe', 'reference']).toContain(classifyKey(key));
      }
    }
  });

  it('switches to vertex_vector_search and validates stream_upsert vs gcs_batch_json write modes', () => {
    const emitted: Array<Record<string, unknown>> = [];
    const handleChange = vi.fn((next) => emitted.push(next));

    const { rerender } = render(
      <VectorStoreSinkProperties node={{ data: { properties: {} } }} onChange={handleChange} />
    );

    fireEvent.change(screen.getByLabelText(/Vector Store Backend/i, { selector: 'select' }), {
      target: { value: 'vertex_vector_search' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        backend: 'vertex_vector_search',
        batchSize: 200,
      })
    );

    rerender(
      <VectorStoreSinkProperties
        node={{
          data: {
            properties: {
              backend: 'vertex_vector_search',
            },
          },
        }}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText(/Vertex AI Location/i, { selector: 'input' })).toHaveValue(
      'us-central1'
    );
    expect(screen.getByLabelText(/Index Write Mode/i, { selector: 'select' })).toHaveValue(
      'stream_upsert'
    );
    expect(screen.getByLabelText(/Upsert Batch Size/i, { selector: 'input' })).toHaveValue(200);
    expect(screen.getByText('GCP project ID is required.')).toBeInTheDocument();
    expect(screen.getByText('Vector Search index name is required.')).toBeInTheDocument();
    expect(
      screen.queryByLabelText(/GCS Batch JSONL Output URI/i, { selector: 'input' })
    ).not.toBeInTheDocument();

    // Switch to gcs_batch_json and verify StoragePathField is required when empty
    rerender(
      <VectorStoreSinkProperties
        node={{
          data: {
            properties: {
              backend: 'vertex_vector_search',
              projectId: 'vertex-rag-proj',
              location: 'us-central1',
              indexName: 'projects/vertex-rag-proj/locations/us-central1/indexes/999',
              writeMode: 'gcs_batch_json',
              gcsOutputUri: '',
            },
          },
        }}
        onChange={handleChange}
      />
    );

    expect(postMessageSpy).toHaveBeenCalledWith({ command: 'getStorageBuckets' });
    expect(
      screen.getByText('GCS output URI is required when write mode is GCS Batch JSONL Export.')
    ).toBeInTheDocument();

    fireEvent.change(
      screen.getByLabelText(/GCS Batch JSONL Output URI/i, { selector: 'input' }),
      {
        target: { name: 'gcsOutputUri', value: 'gs://rag-exports/index-v1/' },
      }
    );
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        backend: 'vertex_vector_search',
        gcsOutputUri: 'gs://rag-exports/index-v1/',
      })
    );

    fireEvent.change(screen.getByLabelText(/Restricts Column/i, { selector: 'input' }), {
      target: { name: 'restrictsColumn', value: 'namespace_restricts' },
    });
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        restrictsColumn: 'namespace_restricts',
      })
    );

    fireEvent.change(screen.getByLabelText(/Crowding Tag Column/i, { selector: 'input' }), {
      target: { name: 'crowdingTagColumn', value: 'author_id' },
    });
    expect(handleChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        crowdingTagColumn: 'author_id',
      })
    );

    for (const payload of emitted) {
      for (const key of Object.keys(payload)) {
        expect(['safe', 'reference']).toContain(classifyKey(key));
      }
    }
  });
});
