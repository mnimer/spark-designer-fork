/**
 * @fileoverview Unit Tests for BigTableIOSinkProperties Component (SNK21).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark BigTableIOSinkProperties rendering, required coordinate and catalog
 * validation, live Bigtable instance and table discovery, createNewTable,
 * writeRowTimestamp, batchMutateSize, enableFlowControl, Secret Manager auth,
 * advanced timeouts, and credential safety.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  BigTableIOSinkProperties,
  BigtableSinkProperties,
  DEFAULT_BIGTABLE_JAR_URI,
} from './BigTableIO';
import { classifyKey } from '../../../../src/credentialRedaction';

describe('BigTableIOSinkProperties (SNK21)', () => {
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

  it('exports BigtableSinkProperties alias and validates required projectId, instanceId, tableId, and catalog', () => {
    expect(BigtableSinkProperties).toBe(BigTableIOSinkProperties);

    const { container } = render(
      <BigTableIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('Spark Cloud Bigtable Connector')).toBeInTheDocument();
    expect(screen.getByText(DEFAULT_BIGTABLE_JAR_URI)).toBeInTheDocument();

    expect(screen.getByText('Project ID is required')).toBeInTheDocument();
    expect(screen.getByText('Instance ID is required')).toBeInTheDocument();
    expect(screen.getByText('Table ID is required')).toBeInTheDocument();
    expect(screen.getByText('Catalog JSON schema mapping is required')).toBeInTheDocument();

    expect(container.querySelector('#bigtableSinkProjectId')).toHaveAttribute('aria-invalid', 'true');
    expect(container.querySelector('#bigtableSinkInstanceId')).toHaveAttribute('aria-invalid', 'true');
    expect(container.querySelector('#bigtableSinkTableId')).toHaveAttribute('aria-invalid', 'true');
    expect(container.querySelector('#bigtableSinkCatalog')).toHaveAttribute('aria-invalid', 'true');

    expect(postMessageSpy).toHaveBeenCalledWith({ command: 'getBigtableInstances' });
    expect(postMessageSpy).toHaveBeenCalledWith({ command: 'getSecrets' });
  });

  it('hydrates properties, syncs tableId/table, and requests bigtableTables when instanceId is present', () => {
    const sampleCatalog = '{"table":{"name":"events"},"rowkey":"k","columns":{}}';
    const node = {
      data: {
        properties: {
          projectId: 'my-write-project',
          instanceId: 'prod-bt',
          tableId: 'events',
          appProfileId: 'writes-profile',
          catalog: sampleCatalog,
          createNewTable: true,
          writeRowTimestamp: 1735689600000,
          batchMutateSize: 2000,
          enableFlowControl: true,
          authMode: 'secret_manager',
          serviceAccountSecret: 'bt-sa-secret',
        },
      },
    };

    const { container } = render(
      <BigTableIOSinkProperties node={node} onChange={mockOnChange} />
    );

    expect(screen.queryAllByRole('alert')).toHaveLength(0);
    expect(postMessageSpy).toHaveBeenCalledWith({
      command: 'getBigtableTables',
      instance: 'prod-bt',
    });

    expect(screen.getByDisplayValue('my-write-project')).toBeInTheDocument();
    expect(screen.getByDisplayValue('prod-bt')).toBeInTheDocument();
    expect(screen.getByDisplayValue('events')).toBeInTheDocument();
    expect(screen.getByDisplayValue('writes-profile')).toBeInTheDocument();
    expect(screen.getByDisplayValue(sampleCatalog)).toBeInTheDocument();
    expect(screen.getByDisplayValue('1735689600000')).toBeInTheDocument();
    expect(screen.getByDisplayValue('2000')).toBeInTheDocument();
    expect(container.querySelector('#bigtableSinkCreateNewTable')).toBeChecked();
    expect(container.querySelector('#bigtableSinkEnableFlowControl')).toBeChecked();
    expect(container.querySelector('#bigtableSinkSecretName')).toHaveValue('bt-sa-secret');
  });

  it('dispatches updates for tableId, catalog, createNewTable, writeRowTimestamp, batchMutateSize, and enableFlowControl', () => {
    const emitted: Array<Record<string, unknown>> = [];
    const onChange = vi.fn((p: Record<string, unknown>) => emitted.push(p));

    const { container } = render(
      <BigTableIOSinkProperties
        node={{
          data: {
            properties: {
              projectId: 'p1',
              instanceId: 'bt1',
              tableId: 'sessions',
              catalog: '{"table":{"name":"sessions"}}',
            },
          },
        }}
        onChange={onChange}
      />
    );

    fireEvent.change(container.querySelector('#bigtableSinkTableId')!, {
      target: { name: 'tableId', value: 'user_metrics' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ tableId: 'user_metrics', table: 'user_metrics' })
    );

    fireEvent.click(container.querySelector('#bigtableSinkCreateNewTable')!);
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ createNewTable: true }));

    fireEvent.change(container.querySelector('#bigtableSinkWriteRowTimestamp')!, {
      target: { name: 'writeRowTimestamp', value: '1700000000000' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ writeRowTimestamp: '1700000000000' })
    );

    fireEvent.change(container.querySelector('#bigtableSinkBatchMutateSize')!, {
      target: { name: 'batchMutateSize', value: '5000' },
    });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ batchMutateSize: '5000' }));

    fireEvent.click(container.querySelector('#bigtableSinkEnableFlowControl')!);
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ enableFlowControl: true }));

    for (const payload of emitted) {
      for (const key of Object.keys(payload)) {
        expect(['safe', 'reference']).toContain(classifyKey(key));
      }
    }
  });

  it('expands Advanced Sink Settings and updates jarUri, emulatorHost, timeouts, and system_instructions', () => {
    const { container } = render(
      <BigTableIOSinkProperties
        node={{
          data: {
            properties: {
              projectId: 'p1',
              instanceId: 'bt1',
              tableId: 'sessions',
              catalog: '{}',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Advanced Sink Settings/i }));

    fireEvent.change(container.querySelector('#bigtableSinkJarUri')!, {
      target: { name: 'jarUri', value: 'gs://my-jars/spark-bigtable.jar' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ jarUri: 'gs://my-jars/spark-bigtable.jar' })
    );

    fireEvent.change(container.querySelector('#bigtableSinkEmulatorHost')!, {
      target: { name: 'emulatorHost', value: 'localhost:8086' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ emulatorHost: 'localhost:8086' })
    );

    fireEvent.change(container.querySelector('#bigtableSinkAttemptTimeoutMs')!, {
      target: { name: 'attemptTimeoutMs', value: '15000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ attemptTimeoutMs: '15000' })
    );

    fireEvent.change(container.querySelector('#bigtableSinkOperationTimeoutMs')!, {
      target: { name: 'operationTimeoutMs', value: '45000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ operationTimeoutMs: '45000' })
    );

    fireEvent.change(container.querySelector('#bigtableSinkInstructions')!, {
      target: { value: 'Map columns to metrics family' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Map columns to metrics family' })
    );
  });
});
