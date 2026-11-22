/**
 * Unit Tests for MongoDbIOSourceProperties Component (SRC32)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates MongoDbIOSourceProperties rendering, readMode ('batch' | 'streaming') switching,
 * database/collection validation, aggregationPipeline, partitioner (batch),
 * changeStreamPublishFullDocumentOnly (streaming), SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MongoDbIOSourceProperties, MongoDbSourceProperties } from './MongoDbIO';

describe('MongoDbIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports MongoDbSourceProperties alias and validates required database and collection when empty', () => {
    expect(MongoDbSourceProperties).toBe(MongoDbIOSourceProperties);
    render(<MongoDbIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Database name is required')).toBeInTheDocument();
    expect(screen.getByText('Collection name is required')).toBeInTheDocument();
  });

  it('hydrates batch properties including aggregationPipeline and partitioner', () => {
    const node = {
      data: {
        properties: {
          readMode: 'batch' as const,
          database: 'analytics',
          collection: 'orders',
          aggregationPipeline: '[{"$match":{"status":"paid"}}]',
          partitioner:
            'com.mongodb.spark.sql.connector.read.partitioner.PaginateBySizePartitioner',
          secretName: 'mongo-uri-secret',
        },
      },
    };

    const { container } = render(<MongoDbIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByDisplayValue('analytics')).toBeInTheDocument();
    expect(screen.getByDisplayValue('orders')).toBeInTheDocument();
    expect(screen.getByDisplayValue('[{"$match":{"status":"paid"}}]')).toBeInTheDocument();
    expect(container.querySelector('#mongoSourcePartitioner')).toHaveValue(
      'com.mongodb.spark.sql.connector.read.partitioner.PaginateBySizePartitioner'
    );
  });

  it('switches to streaming mode and displays changeStreamPublishFullDocumentOnly toggle', () => {
    const { container, rerender } = render(
      <MongoDbIOSourceProperties
        node={{
          data: {
            properties: {
              readMode: 'batch',
              database: 'analytics',
              collection: 'orders',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const modeSelect = container.querySelector('#mongoSourceReadMode') as HTMLSelectElement;
    fireEvent.change(modeSelect, { target: { name: 'readMode', value: 'streaming' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ readMode: 'streaming' }));

    rerender(
      <MongoDbIOSourceProperties
        node={{
          data: {
            properties: {
              readMode: 'streaming',
              database: 'analytics',
              collection: 'orders',
              changeStreamPublishFullDocumentOnly: true,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const fullDocCheckbox = container.querySelector(
      '#mongoSourceChangeStreamFullDocumentOnly'
    ) as HTMLInputElement;
    expect(fullDocCheckbox).toBeChecked();

    fireEvent.click(fullDocCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ changeStreamPublishFullDocumentOnly: false })
    );
  });

  it('updates system_instructions when instructions textarea is edited', () => {
    const { container } = render(
      <MongoDbIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );
    const textarea = container.querySelector('#mongoSourceInstructions') as HTMLTextAreaElement;
    fireEvent.change(textarea, { target: { value: 'Parse BSON timestamps' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Parse BSON timestamps' })
    );
  });
});
