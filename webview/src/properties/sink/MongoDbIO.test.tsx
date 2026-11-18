/**
 * @fileoverview Unit Tests for MongoDbIOSinkProperties Component (SNK29).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates MongoDbIOSinkProperties rendering, database and collection validation,
 * operationType ('insert' | 'replace' | 'update') switching, idFieldList validation,
 * maxBatchSize and ordered bulk settings, SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MongoDbIOSinkProperties, MongoDbSinkProperties } from './MongoDbIO';

describe('MongoDbIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports MongoDbSinkProperties alias and validates required database and collection when empty', () => {
    expect(MongoDbSinkProperties).toBe(MongoDbIOSinkProperties);
    render(<MongoDbIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Database name is required')).toBeInTheDocument();
    expect(screen.getByText('Collection name is required')).toBeInTheDocument();
  });

  it('hydrates Spark MongoDB sink properties including replace operation and bulk settings', () => {
    const node = {
      data: {
        properties: {
          database: 'telemetry',
          collection: 'hourly_summary',
          saveMode: 'overwrite' as const,
          operationType: 'replace' as const,
          idFieldList: 'device_id,hour_bucket',
          upsertDocument: false,
          maxBatchSize: 1024,
          ordered: false,
          writeConcern: '1',
          secretName: 'mongo-write-uri-secret',
        },
      },
    };

    const { container } = render(<MongoDbIOSinkProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByDisplayValue('telemetry')).toBeInTheDocument();
    expect(screen.getByDisplayValue('hourly_summary')).toBeInTheDocument();
    expect(container.querySelector('#mongoSinkSaveMode')).toHaveValue('overwrite');
    expect(container.querySelector('#mongoSinkOperationType')).toHaveValue('replace');
    expect(screen.getByDisplayValue('device_id,hour_bucket')).toBeInTheDocument();
    expect(container.querySelector('#mongoSinkUpsertDocument')).not.toBeChecked();
    expect(screen.getByDisplayValue('1024')).toBeInTheDocument();
    expect(container.querySelector('#mongoSinkOrdered')).not.toBeChecked();
    expect(container.querySelector('#mongoSinkWriteConcern')).toHaveValue('1');
    expect(screen.getByDisplayValue('mongo-write-uri-secret')).toBeInTheDocument();
  });

  it('switches operationType to update and validates required idFieldList when empty', () => {
    const { container, rerender } = render(
      <MongoDbIOSinkProperties
        node={{
          data: {
            properties: {
              database: 'analytics',
              collection: 'users',
              operationType: 'insert',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#mongoSinkIdFieldList')).not.toBeInTheDocument();

    const opSelect = container.querySelector('#mongoSinkOperationType') as HTMLSelectElement;
    fireEvent.change(opSelect, { target: { name: 'operationType', value: 'update' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ operationType: 'update' })
    );

    rerender(
      <MongoDbIOSinkProperties
        node={{
          data: {
            properties: {
              database: 'analytics',
              collection: 'users',
              operationType: 'update',
              idFieldList: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText('Key field list is required for replace or update operations')
    ).toBeInTheDocument();

    const idInput = container.querySelector('#mongoSinkIdFieldList') as HTMLInputElement;
    fireEvent.change(idInput, { target: { name: 'idFieldList', value: '_id' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ idFieldList: '_id' }));
  });

  it('updates maxBatchSize, ordered toggle, and custom connection URI warning', () => {
    const { container } = render(
      <MongoDbIOSinkProperties
        node={{
          data: {
            properties: {
              database: 'analytics',
              collection: 'events',
              authMode: 'default',
              uri: 'http://invalid-host',
              ordered: true,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText('Connection URI must start with mongodb:// or mongodb+srv://')
    ).toBeInTheDocument();

    const batchInput = container.querySelector('#mongoSinkMaxBatchSize') as HTMLInputElement;
    fireEvent.change(batchInput, { target: { name: 'maxBatchSize', value: '256' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ maxBatchSize: '256' }));

    const orderedCheckbox = container.querySelector('#mongoSinkOrdered') as HTMLInputElement;
    fireEvent.click(orderedCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ ordered: false }));
  });

  it('updates system_instructions and handles missing properties gracefully', () => {
    const { container } = render(
      <MongoDbIOSinkProperties node={{ data: {} }} onChange={mockOnChange} />
    );
    const textarea = container.querySelector('#mongoSinkInstructions') as HTMLTextAreaElement;
    expect(textarea).toHaveValue('');

    fireEvent.change(textarea, { target: { value: 'Convert JSON payload to BSON document' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Convert JSON payload to BSON document' })
    );
  });
});
