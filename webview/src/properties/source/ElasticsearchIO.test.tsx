/**
 * Unit Tests for ElasticsearchIOSourceProperties Component (SRC36)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates ElasticsearchIOSourceProperties rendering, nodes/port/resource/query/wanOnly,
 * SecretManagerAuthFields integration, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ElasticsearchIOSourceProperties, ElasticsearchSourceProperties } from './ElasticsearchIO';

describe('ElasticsearchIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports ElasticsearchSourceProperties alias and validates required nodes and resource when empty', () => {
    expect(ElasticsearchSourceProperties).toBe(ElasticsearchIOSourceProperties);
    render(<ElasticsearchIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Elasticsearch nodes are required')).toBeInTheDocument();
    expect(screen.getByText('Index resource is required')).toBeInTheDocument();
    expect(screen.getByDisplayValue('9200')).toBeInTheDocument();
  });

  it('hydrates nodes, port, resource, query, and wanOnly and dispatches updates', () => {
    const node = {
      data: {
        properties: {
          nodes: 'es.internal',
          port: 9200,
          resource: 'sensor_readings',
          query: '{"query":{"match_all":{}}}',
          wanOnly: true,
        },
      },
    };

    const { container } = render(
      <ElasticsearchIOSourceProperties node={node} onChange={mockOnChange} />
    );

    expect(screen.getByDisplayValue('es.internal')).toBeInTheDocument();
    expect(screen.getByDisplayValue('sensor_readings')).toBeInTheDocument();
    expect(screen.getByDisplayValue('{"query":{"match_all":{}}}')).toBeInTheDocument();

    const wanCheckbox = container.querySelector('#esSourceWanOnly') as HTMLInputElement;
    expect(wanCheckbox).toBeChecked();

    fireEvent.click(wanCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ wanOnly: false }));

    const resInput = container.querySelector('#esSourceResource') as HTMLInputElement;
    fireEvent.change(resInput, { target: { name: 'resource', value: 'orders-idx' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ resource: 'orders-idx', index: 'orders-idx' })
    );

    const sysInput = container.querySelector('#es-source-instructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Push down date range' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Push down date range' })
    );
  });
});
