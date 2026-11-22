/**
 * @fileoverview Unit Tests for ElasticsearchIOSinkProperties Component (SNK33).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates ElasticsearchIOSinkProperties rendering, nodes and resource validation,
 * writeOperation ('index' | 'upsert' | 'create' | 'update') switching, mappingId requirement,
 * wanOnly and ssl toggles, bulk batch sizing, SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ElasticsearchIOSinkProperties, ElasticsearchSinkProperties } from './ElasticsearchIO';

describe('ElasticsearchIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports ElasticsearchSinkProperties alias and validates required nodes and resource when empty', () => {
    expect(ElasticsearchSinkProperties).toBe(ElasticsearchIOSinkProperties);
    render(
      <ElasticsearchIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('Elasticsearch nodes are required')).toBeInTheDocument();
    expect(screen.getByText('Index resource is required')).toBeInTheDocument();
    expect(screen.getByDisplayValue('9200')).toBeInTheDocument();
    expect(screen.getByDisplayValue('1000')).toBeInTheDocument();
  });

  it('hydrates Elasticsearch sink properties including wanOnly, ssl, upsert, and batch sizing', () => {
    const node = {
      data: {
        properties: {
          nodes: 'es.cloud.internal',
          port: 443,
          resource: 'orders-v1',
          wanOnly: true,
          ssl: true,
          writeOperation: 'upsert' as const,
          mappingId: 'order_id',
          batchSizeEntries: 2500,
          batchSizeBytes: '10mb',
          username: 'elastic',
          authMode: 'secretManager',
          secretName: 'es-prod-password',
        },
      },
    };

    const { container } = render(
      <ElasticsearchIOSinkProperties node={node} onChange={mockOnChange} />
    );

    expect(screen.getByDisplayValue('es.cloud.internal')).toBeInTheDocument();
    expect(screen.getByDisplayValue('443')).toBeInTheDocument();
    expect(screen.getByDisplayValue('orders-v1')).toBeInTheDocument();
    expect(container.querySelector('#esSinkWanOnly')).toBeChecked();
    expect(container.querySelector('#esSinkSsl')).toBeChecked();
    expect(container.querySelector('#esSinkWriteOperation')).toHaveValue('upsert');
    expect(screen.getByDisplayValue('order_id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('2500')).toBeInTheDocument();
    expect(screen.getByDisplayValue('10mb')).toBeInTheDocument();
    expect(screen.getByDisplayValue('elastic')).toBeInTheDocument();
    expect(screen.getByDisplayValue('es-prod-password')).toBeInTheDocument();
  });

  it('requires mappingId when writeOperation is upsert, create, or update and syncs index on resource change', () => {
    const { container, rerender } = render(
      <ElasticsearchIOSinkProperties
        node={{
          data: {
            properties: {
              nodes: 'es.internal',
              resource: 'metrics',
              writeOperation: 'index',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const opSelect = container.querySelector('#esSinkWriteOperation') as HTMLSelectElement;
    fireEvent.change(opSelect, { target: { name: 'writeOperation', value: 'upsert' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ writeOperation: 'upsert' })
    );

    rerender(
      <ElasticsearchIOSinkProperties
        node={{
          data: {
            properties: {
              nodes: 'es.internal',
              resource: 'metrics',
              writeOperation: 'upsert',
              mappingId: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText('Document ID column is required for upsert, create, or update')
    ).toBeInTheDocument();

    const resInput = container.querySelector('#esSinkResource') as HTMLInputElement;
    fireEvent.change(resInput, { target: { name: 'resource', value: 'metrics-daily' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ resource: 'metrics-daily', index: 'metrics-daily' })
    );

    const sysInput = container.querySelector('#esSinkInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Exclude internal metadata columns' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Exclude internal metadata columns' })
    );
  });
});
