/**
 * Unit Tests for CloudSQLSourceProperties Component (SRC25)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies CloudSQLSourceProperties rendering, instanceConnectionName validation,
 * dbEngine selection, dynamicBounds toggle, SecretManagerAuthFields integration,
 * driverJarUri/socketFactoryJarUri fields, and credential safety.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { CloudSQLSourceProperties, CloudSqlSourceProperties } from './CloudSQL';
import { classifyKey } from '../../../../src/credentialRedaction';

describe('CloudSQLSourceProperties', () => {
  const mockOnChange = vi.fn<(properties: Record<string, unknown>) => void>();
  let postMessageSpy: ReturnType<typeof vi.fn<(message: unknown) => void>>;

  beforeEach(() => {
    vi.clearAllMocks();
    postMessageSpy = vi.fn<(message: unknown) => void>();
    window.vscode = { postMessage: postMessageSpy };
  });

  afterEach(() => {
    delete window.vscode;
  });

  it('exports CloudSqlSourceProperties alias and validates required fields when empty', () => {
    expect(CloudSqlSourceProperties).toBe(CloudSQLSourceProperties);
    render(<CloudSQLSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Instance connection name is required')).toBeInTheDocument();
    expect(screen.getByText('Database is required')).toBeInTheDocument();
    expect(screen.getByText('Table name is required in Table mode')).toBeInTheDocument();
  });

  it('hydrates properties and toggles dynamicBounds vs manual lowerBound/upperBound', () => {
    const node = {
      data: {
        properties: {
          instanceConnectionName: 'my-proj:us-central1:pg-inst',
          dbEngine: 'postgresql' as const,
          database: 'orders_db',
          table: 'public.orders',
          partitionColumn: 'order_id',
          numPartitions: 8,
          dynamicBounds: true,
          fetchsize: 5000,
          authMode: 'iam',
          driverJarUri: 'gs://my-bucket/jars/postgresql.jar',
          socketFactoryJarUri: 'gs://my-bucket/jars/socket-factory.jar',
        },
      },
    };

    const { container, rerender } = render(
      <CloudSQLSourceProperties node={node} onChange={mockOnChange} />
    );

    expect(screen.getByDisplayValue('my-proj:us-central1:pg-inst')).toBeInTheDocument();
    expect(screen.getByDisplayValue('orders_db')).toBeInTheDocument();
    expect(screen.getByDisplayValue('public.orders')).toBeInTheDocument();
    expect(screen.getByDisplayValue('order_id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('8')).toBeInTheDocument();
    expect(screen.getByDisplayValue('5000')).toBeInTheDocument();

    const dynamicCheckbox = container.querySelector('#cloudSqlSourceDynamicBounds') as HTMLInputElement;
    expect(dynamicCheckbox).toBeChecked();
    expect(container.querySelector('#cloudSqlSourceLowerBound')).not.toBeInTheDocument();

    fireEvent.click(dynamicCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ dynamicBounds: false }));

    rerender(
      <CloudSQLSourceProperties
        node={{
          data: {
            properties: {
              ...node.data.properties,
              dynamicBounds: false,
              lowerBound: 1,
              upperBound: 10000,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#cloudSqlSourceLowerBound')).toHaveValue(1);
    expect(container.querySelector('#cloudSqlSourceUpperBound')).toHaveValue(10000);
  });

  it('supports secret_manager authMode and ensures all emitted keys classify safely', () => {
    const emitted: Array<Record<string, unknown>> = [];
    const onChange = vi.fn((p: Record<string, unknown>) => emitted.push(p));

    const { container } = render(
      <CloudSQLSourceProperties
        node={{
          data: {
            properties: {
              instanceConnectionName: 'my-proj:us-central1:pg-inst',
              dbEngine: 'sqlserver',
              database: 'sales',
              readTargetMode: 'query',
              query: 'SELECT * FROM sales',
              authMode: 'secret_manager',
              secretName: 'sql-pw-secret',
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(screen.getByText(/dataproc\.artifacts\.remove=conscrypt/i)).toBeInTheDocument();
    const secretInput = container.querySelector('#cloudSqlSourceSecretName') as HTMLInputElement;
    expect(secretInput).toHaveValue('sql-pw-secret');

    fireEvent.change(secretInput, { target: { name: 'secretName', value: 'new-sql-secret' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ secretName: 'new-sql-secret' }));

    for (const payload of emitted) {
      for (const key of Object.keys(payload)) {
        expect(['safe', 'reference']).toContain(classifyKey(key));
      }
    }
  });
});
