/**
 * @fileoverview Unit Tests for CloudSQLSinkProperties Component (SNK22).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies Spark CloudSQLSinkProperties rendering, live GCP sqlInstances and gated
 * sqlDatabases discovery, dbEngine selection (including SQL Server Conscrypt warning),
 * saveMode with safe truncate=true toggle, batchsize=5000, isolationLevel, IAM vs
 * Secret Manager authentication, runtime JAR URIs, and credential redaction safety.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { CloudSQLSinkProperties, CloudSqlSinkProperties } from './CloudSQL';
import { classifyKey } from '../../../../src/credentialRedaction';

describe('CloudSQLSinkProperties (SNK22)', () => {
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

  it('exports CloudSqlSinkProperties alias, requests sqlInstances, and validates required fields', () => {
    expect(CloudSqlSinkProperties).toBe(CloudSQLSinkProperties);

    const { container } = render(
      <CloudSQLSinkProperties node={{ data: { properties: {} } }} onChange={vi.fn()} />
    );

    expect(screen.getByText('Spark Cloud SQL JDBC + Socket Factory')).toBeInTheDocument();
    expect(screen.getByText('Instance connection name is required')).toBeInTheDocument();
    expect(screen.getByText('Database is required')).toBeInTheDocument();
    expect(screen.getByText('Table name is required')).toBeInTheDocument();

    expect(container.querySelector('#cloudSqlSinkBatchsize')).toHaveValue(5000);
    expect(postMessageSpy).toHaveBeenCalledWith({ command: 'getSqlInstances' });
    expect(postMessageSpy).not.toHaveBeenCalledWith(
      expect.objectContaining({ command: 'getSqlDatabases' })
    );
  });

  it('gates sqlDatabases discovery on well-formed connection name and surfaces discovery alerts', () => {
    const { rerender } = render(
      <CloudSQLSinkProperties
        node={{
          data: {
            properties: {
              instanceConnectionName: 'malformed-name',
              database: 'analytics',
              table: 'public.orders',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(
      screen.getByText('Use the instance connection name PROJECT:REGION:INSTANCE')
    ).toBeInTheDocument();

    rerender(
      <CloudSQLSinkProperties
        node={{
          data: {
            properties: {
              instanceConnectionName: 'my-proj:us-central1:pg-prod',
              database: 'analytics',
              table: 'public.orders',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(postMessageSpy).toHaveBeenCalledWith({
      command: 'getSqlDatabases',
      instance: 'my-proj:us-central1:pg-prod',
    });

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'sqlDatabasesLoaded',
            instance: 'my-proj:us-central1:pg-prod',
            sqlDatabases: ['postgres', 'analytics'],
          },
        })
      );
    });

    const dbOptions = Array.from(
      document.querySelectorAll('#cloudsql-sink-database-options option')
    ).map((opt) => opt.getAttribute('value'));
    expect(dbOptions).toEqual(['postgres', 'analytics']);
  });

  it('shows safe truncate=true checkbox on overwrite mode and Conscrypt warning on sqlserver', () => {
    const mockOnChange = vi.fn();
    const { container, rerender } = render(
      <CloudSQLSinkProperties
        node={{
          data: {
            properties: {
              instanceConnectionName: 'my-proj:us-central1:sqlsrv-inst',
              dbEngine: 'sqlserver',
              database: 'sales_db',
              table: 'dbo.Orders',
              saveMode: 'overwrite',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText(/dataproc\.artifacts\.remove=conscrypt/i)).toBeInTheDocument();

    const truncateCheckbox = container.querySelector('#cloudSqlSinkTruncate') as HTMLInputElement;
    expect(truncateCheckbox).toBeInTheDocument();
    expect(truncateCheckbox).toBeChecked();

    fireEvent.click(truncateCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ truncate: false }));

    rerender(
      <CloudSQLSinkProperties
        node={{
          data: {
            properties: {
              instanceConnectionName: 'my-proj:us-central1:pg-inst',
              dbEngine: 'postgresql',
              database: 'sales_db',
              table: 'public.orders',
              saveMode: 'append',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#cloudSqlSinkTruncate')).not.toBeInTheDocument();
    expect(screen.queryByText(/dataproc\.artifacts\.remove=conscrypt/i)).not.toBeInTheDocument();
  });

  it('dispatches updates for saveMode, isolationLevel, batchsize, numPartitions, Secret Manager, JAR URIs, and system_instructions safely', () => {
    const emitted: Array<Record<string, unknown>> = [];
    const onChange = vi.fn((p: Record<string, unknown>) => emitted.push(p));

    const { container } = render(
      <CloudSQLSinkProperties
        node={{
          data: {
            properties: {
              cloudSqlInstance: 'my-proj:us-central1:mysql-inst',
              dbEngine: 'mysql',
              database: 'inventory',
              table: 'stock_levels',
              authMode: 'secret_manager',
              passwordSecret: 'legacy-mysql-pw',
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(container.querySelector('input[type="password"]')).toBeNull();

    fireEvent.change(container.querySelector('#cloudSqlSinkInstance')!, {
      target: { name: 'instanceConnectionName', value: 'my-proj:us-central1:mysql-v2' },
    });
    expect(onChange).toHaveBeenLastCalledWith(
      expect.objectContaining({ instanceConnectionName: 'my-proj:us-central1:mysql-v2' })
    );
    expect(emitted[emitted.length - 1]).not.toHaveProperty('cloudSqlInstance');

    fireEvent.change(container.querySelector('#cloudSqlSinkSaveMode')!, {
      target: { name: 'saveMode', value: 'overwrite' },
    });
    expect(onChange).toHaveBeenLastCalledWith(expect.objectContaining({ saveMode: 'overwrite' }));

    fireEvent.change(container.querySelector('#cloudSqlSinkIsolationLevel')!, {
      target: { name: 'isolationLevel', value: 'REPEATABLE_READ' },
    });
    expect(onChange).toHaveBeenLastCalledWith(
      expect.objectContaining({ isolationLevel: 'REPEATABLE_READ' })
    );

    fireEvent.change(container.querySelector('#cloudSqlSinkBatchsize')!, {
      target: { name: 'batchsize', value: '10000' },
    });
    expect(onChange).toHaveBeenLastCalledWith(expect.objectContaining({ batchsize: '10000' }));

    fireEvent.change(container.querySelector('#cloudSqlSinkNumPartitions')!, {
      target: { name: 'numPartitions', value: '8' },
    });
    expect(onChange).toHaveBeenLastCalledWith(expect.objectContaining({ numPartitions: '8' }));

    fireEvent.change(container.querySelector('#cloudSqlSinkSecretName')!, {
      target: { name: 'secretName', value: 'mysql-writer-secret' },
    });
    expect(onChange).toHaveBeenLastCalledWith(
      expect.objectContaining({ secretName: 'mysql-writer-secret' })
    );
    expect(emitted[emitted.length - 1]).not.toHaveProperty('passwordSecret');

    fireEvent.change(container.querySelector('#cloudSqlSinkDriverJarUri')!, {
      target: { name: 'driverJarUri', value: 'gs://jars/mysql-connector-j.jar' },
    });
    fireEvent.change(container.querySelector('#cloudSqlSinkSocketFactoryJarUri')!, {
      target: { name: 'socketFactoryJarUri', value: 'gs://jars/mysql-socket-factory.jar' },
    });

    fireEvent.change(container.querySelector('#cloudSqlSinkSystemInstructions')!, {
      target: { value: 'Coalesce to 4 partitions before writing' },
    });
    expect(onChange).toHaveBeenLastCalledWith(
      expect.objectContaining({
        system_instructions: 'Coalesce to 4 partitions before writing',
      })
    );

    for (const payload of emitted) {
      for (const key of Object.keys(payload)) {
        expect(['safe', 'reference']).toContain(classifyKey(key));
      }
    }
  });
});
