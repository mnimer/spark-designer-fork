/**
 * @fileoverview Unit Tests for SpannerIOSinkProperties Component (SNK20).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark SpannerIOSinkProperties rendering, required coordinate validation
 * (projectId, instanceId, databaseId, table), saveMode and mutationMode selection,
 * Secret Manager authentication, collapsible advanced settings, and credential safety.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  SpannerIOSinkProperties,
  SpannerSinkProperties,
  DEFAULT_SPANNER_JAR_URI,
} from './SpannerIO';
import { classifyKey } from '../../../../src/credentialRedaction';

describe('SpannerIOSinkProperties (SNK20)', () => {
  const mockOnChange = vi.fn();
  let postMessageSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.clearAllMocks();
    postMessageSpy = vi.fn();
    (window as unknown as { vscode?: { postMessage: typeof postMessageSpy } }).vscode = {
      postMessage: postMessageSpy,
    };
  });

  afterEach(() => {
    delete (window as unknown as { vscode?: unknown }).vscode;
  });

  it('exports SpannerSinkProperties alias, displays connector JAR badge, and validates required fields', () => {
    expect(SpannerSinkProperties).toBe(SpannerIOSinkProperties);

    const { container } = render(
      <SpannerIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('Spark Cloud Spanner Connector')).toBeInTheDocument();
    expect(screen.getByText(DEFAULT_SPANNER_JAR_URI)).toBeInTheDocument();

    expect(screen.getByText('Project ID is required')).toBeInTheDocument();
    expect(screen.getByText('Spanner instance ID is required')).toBeInTheDocument();
    expect(screen.getByText('Spanner database ID is required')).toBeInTheDocument();
    expect(screen.getByText('Table name is required')).toBeInTheDocument();

    expect(container.querySelector('#spannerSinkProjectId')).toHaveAttribute('aria-invalid', 'true');
    expect(container.querySelector('#spannerSinkInstanceId')).toHaveAttribute('aria-invalid', 'true');
    expect(container.querySelector('#spannerSinkDatabaseId')).toHaveAttribute('aria-invalid', 'true');
    expect(container.querySelector('#spannerSinkTable')).toHaveAttribute('aria-invalid', 'true');

    expect(postMessageSpy).toHaveBeenCalledWith({ command: 'getSecrets' });
  });

  it('hydrates properties and normalizes legacy insertOrUpdate and secret_manager values', () => {
    const node = {
      data: {
        properties: {
          projectId: 'my-gcp-project',
          instanceId: 'prod-spanner',
          databaseId: 'orders_db',
          table: 'Orders',
          saveMode: 'overwrite' as const,
          mutationMode: 'insertOrUpdate',
          mutationsPerTransaction: 4000,
          authMode: 'secret_manager',
          serviceAccountKeySecret: 'spanner-writer-sa',
        },
      },
    };

    const { container } = render(
      <SpannerIOSinkProperties node={node} onChange={mockOnChange} />
    );

    expect(screen.queryAllByRole('alert')).toHaveLength(0);
    expect(screen.getByDisplayValue('my-gcp-project')).toBeInTheDocument();
    expect(screen.getByDisplayValue('prod-spanner')).toBeInTheDocument();
    expect(screen.getByDisplayValue('orders_db')).toBeInTheDocument();
    expect(screen.getByDisplayValue('Orders')).toBeInTheDocument();
    expect(screen.getByDisplayValue('4000')).toBeInTheDocument();

    expect((container.querySelector('#spannerSinkSaveMode') as HTMLSelectElement).value).toBe(
      'overwrite'
    );
    expect((container.querySelector('#spannerSinkMutationMode') as HTMLSelectElement).value).toBe(
      'insert_or_update'
    );
    expect(container.querySelector('#spannerSinkSecretName')).toHaveValue('spanner-writer-sa');
  });

  it('dispatches updates for coordinates, saveMode, mutationMode, and mutationsPerTransaction', () => {
    const emitted: Array<Record<string, unknown>> = [];
    const onChange = vi.fn((p: Record<string, unknown>) => emitted.push(p));

    const { container } = render(
      <SpannerIOSinkProperties
        node={{
          data: {
            properties: {
              projectId: 'p1',
              instanceId: 'inst1',
              databaseId: 'db1',
              table: 'Orders',
            },
          },
        }}
        onChange={onChange}
      />
    );

    fireEvent.change(container.querySelector('#spannerSinkTable')!, {
      target: { name: 'table', value: 'LineItems' },
    });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ table: 'LineItems' }));

    fireEvent.change(container.querySelector('#spannerSinkSaveMode')!, {
      target: { name: 'saveMode', value: 'errorifexists' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ saveMode: 'errorifexists' })
    );

    fireEvent.change(container.querySelector('#spannerSinkMutationMode')!, {
      target: { name: 'mutationMode', value: 'replace' },
    });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ mutationMode: 'replace' }));

    fireEvent.change(container.querySelector('#spannerSinkMutationsPerTransaction')!, {
      target: { name: 'mutationsPerTransaction', value: '2500' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ mutationsPerTransaction: '2500' })
    );

    for (const payload of emitted) {
      for (const key of Object.keys(payload)) {
        expect(['safe', 'reference']).toContain(classifyKey(key));
      }
    }
  });

  it('expands Advanced Sink Settings and updates jarUri, databaseRole, emulatorHost, commitTimeoutSeconds, and system_instructions', () => {
    const { container } = render(
      <SpannerIOSinkProperties
        node={{
          data: {
            properties: {
              projectId: 'p1',
              instanceId: 'inst1',
              databaseId: 'db1',
              table: 'Orders',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Advanced Sink Settings/i }));

    fireEvent.change(container.querySelector('#spannerSinkJarUri')!, {
      target: { name: 'jarUri', value: 'gs://custom-bucket/spanner.jar' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ jarUri: 'gs://custom-bucket/spanner.jar' })
    );

    fireEvent.change(container.querySelector('#spannerSinkDatabaseRole')!, {
      target: { name: 'databaseRole', value: 'writer_role' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ databaseRole: 'writer_role' })
    );

    fireEvent.change(container.querySelector('#spannerSinkEmulatorHost')!, {
      target: { name: 'emulatorHost', value: 'localhost:9010' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ emulatorHost: 'localhost:9010' })
    );

    fireEvent.change(container.querySelector('#spannerSinkCommitTimeoutSeconds')!, {
      target: { name: 'commitTimeoutSeconds', value: '90' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ commitTimeoutSeconds: '90' })
    );

    fireEvent.change(container.querySelector('#spannerSinkSystemInstructions')!, {
      target: { value: 'Use insert_or_update inside foreachBatch' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Use insert_or_update inside foreachBatch',
      })
    );
  });
});
