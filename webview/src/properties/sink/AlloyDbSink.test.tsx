/**
 * @fileoverview Unit Tests for AlloyDbSinkProperties Component (SNK23).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates AlloyDbSinkProperties rendering, Conscrypt removal note, alloydbInstanceUri
 * format validation, ipType selector, saveMode with safe truncate=true default on
 * overwrite, batchsize=5000, isolationLevel, IAM vs Secret Manager authentication,
 * driverJarUri and socketFactoryJarUri inputs, and credential redaction safety.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { AlloyDbSinkProperties } from './AlloyDbSink';
import { classifyKey } from '../../../../src/credentialRedaction';

describe('AlloyDbSinkProperties (SNK23)', () => {
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

  it('renders Conscrypt helper note, batchsize=5000 default, and validates required fields when empty', () => {
    const { container } = render(
      <AlloyDbSinkProperties node={{ data: { properties: {} } }} onChange={vi.fn()} />
    );

    expect(screen.getByText('Spark AlloyDB JDBC + Socket Factory')).toBeInTheDocument();
    expect(screen.getByText(/dataproc\.artifacts\.remove=conscrypt/i)).toBeInTheDocument();
    expect(screen.getByText('AlloyDB instance URI is required')).toBeInTheDocument();
    expect(screen.getByText('Database is required')).toBeInTheDocument();
    expect(screen.getByText('Table name is required')).toBeInTheDocument();
    expect(container.querySelector('#alloydbSinkBatchsize')).toHaveValue(5000);
  });

  it('validates malformed alloydbInstanceUri, database, and table, and accepts canonical configuration', () => {
    const { rerender } = render(
      <AlloyDbSinkProperties
        node={{
          data: {
            properties: {
              alloydbInstanceUri: 'invalid-uri',
              database: 'bad/db',
              table: 'bad table',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(
      screen.getByText(
        'Use projects/<project>/locations/<region>/clusters/<cluster>/instances/<instance>'
      )
    ).toBeInTheDocument();
    expect(
      screen.getByText('Database name cannot contain spaces or / ? # @ :')
    ).toBeInTheDocument();
    expect(screen.getByText('Table name cannot contain whitespace')).toBeInTheDocument();

    rerender(
      <AlloyDbSinkProperties
        node={{
          data: {
            properties: {
              alloydbInstanceUri:
                'projects/my-proj/locations/us-central1/clusters/my-cluster/instances/my-inst',
              ipType: 'PSC',
              database: 'postgres',
              table: 'public.ledger',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.queryAllByRole('alert')).toHaveLength(0);
    expect(screen.getByDisplayValue('PSC (Private Service Connect)')).toBeInTheDocument();
  });

  it('toggles safe truncate=true checkbox when saveMode is overwrite', () => {
    const mockOnChange = vi.fn();
    const { container, rerender } = render(
      <AlloyDbSinkProperties
        node={{
          data: {
            properties: {
              alloydbInstanceUri:
                'projects/my-proj/locations/us-central1/clusters/c1/instances/i1',
              database: 'postgres',
              table: 'public.ledger',
              saveMode: 'append',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#alloydbSinkTruncate')).not.toBeInTheDocument();

    rerender(
      <AlloyDbSinkProperties
        node={{
          data: {
            properties: {
              alloydbInstanceUri:
                'projects/my-proj/locations/us-central1/clusters/c1/instances/i1',
              database: 'postgres',
              table: 'public.ledger',
              saveMode: 'overwrite',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const truncateCheckbox = container.querySelector('#alloydbSinkTruncate') as HTMLInputElement;
    expect(truncateCheckbox).toBeInTheDocument();
    expect(truncateCheckbox).toBeChecked();

    fireEvent.click(truncateCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ truncate: false }));
  });

  it('dispatches updates for ipType, saveMode, isolationLevel, batchsize, numPartitions, Secret Manager, JAR URIs, and system_instructions safely', () => {
    const emitted: Array<Record<string, unknown>> = [];
    const onChange = vi.fn((p: Record<string, unknown>) => emitted.push(p));

    const { container } = render(
      <AlloyDbSinkProperties
        node={{
          data: {
            properties: {
              alloydbInstanceName:
                'projects/my-proj/locations/us-central1/clusters/c1/instances/i1',
              database: 'postgres',
              table: 'public.ledger',
              authMode: 'secret_manager',
              passwordSecret: 'old-alloydb-pw',
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(container.querySelector('input[type="password"]')).toBeNull();
    expect(postMessageSpy).toHaveBeenCalledWith({ command: 'getSecrets' });

    fireEvent.change(container.querySelector('#alloydbSinkInstanceUri')!, {
      target: {
        name: 'alloydbInstanceUri',
        value: 'projects/my-proj/locations/us-central1/clusters/c1/instances/i2',
      },
    });
    expect(onChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        alloydbInstanceUri: 'projects/my-proj/locations/us-central1/clusters/c1/instances/i2',
      })
    );
    expect(emitted[emitted.length - 1]).not.toHaveProperty('alloydbInstanceName');

    fireEvent.change(container.querySelector('#alloydbSinkIpType')!, {
      target: { name: 'ipType', value: 'PUBLIC' },
    });
    expect(onChange).toHaveBeenLastCalledWith(expect.objectContaining({ ipType: 'PUBLIC' }));

    fireEvent.change(container.querySelector('#alloydbSinkSaveMode')!, {
      target: { name: 'saveMode', value: 'errorifexists' },
    });
    expect(onChange).toHaveBeenLastCalledWith(
      expect.objectContaining({ saveMode: 'errorifexists' })
    );

    fireEvent.change(container.querySelector('#alloydbSinkIsolationLevel')!, {
      target: { name: 'isolationLevel', value: 'SERIALIZABLE' },
    });
    expect(onChange).toHaveBeenLastCalledWith(
      expect.objectContaining({ isolationLevel: 'SERIALIZABLE' })
    );

    fireEvent.change(container.querySelector('#alloydbSinkBatchsize')!, {
      target: { name: 'batchsize', value: '10000' },
    });
    expect(onChange).toHaveBeenLastCalledWith(expect.objectContaining({ batchsize: '10000' }));

    fireEvent.change(container.querySelector('#alloydbSinkNumPartitions')!, {
      target: { name: 'numPartitions', value: '12' },
    });
    expect(onChange).toHaveBeenLastCalledWith(expect.objectContaining({ numPartitions: '12' }));

    fireEvent.change(container.querySelector('#alloydbSinkSecretName')!, {
      target: { name: 'secretName', value: 'alloydb-writer-secret' },
    });
    expect(onChange).toHaveBeenLastCalledWith(
      expect.objectContaining({ secretName: 'alloydb-writer-secret' })
    );
    expect(emitted[emitted.length - 1]).not.toHaveProperty('passwordSecret');

    fireEvent.change(container.querySelector('#alloydbSinkDriverJarUri')!, {
      target: { name: 'driverJarUri', value: 'gs://jars/postgresql-42.7.4.jar' },
    });
    fireEvent.change(container.querySelector('#alloydbSinkSocketFactoryJarUri')!, {
      target: { name: 'socketFactoryJarUri', value: 'gs://jars/alloydb-jdbc-connector.jar' },
    });

    fireEvent.change(container.querySelector('#alloydbSinkSystemInstructions')!, {
      target: { value: 'Use AlloyDB PSC with IAM authentication' },
    });
    expect(onChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        system_instructions: 'Use AlloyDB PSC with IAM authentication',
      })
    );

    for (const payload of emitted) {
      for (const key of Object.keys(payload)) {
        expect(['safe', 'reference']).toContain(classifyKey(key));
      }
    }
  });
});
