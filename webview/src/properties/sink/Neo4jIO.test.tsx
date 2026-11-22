/**
 * @fileoverview Unit Tests for Neo4jIOSinkProperties Component (SNK34).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Neo4jIOSinkProperties rendering, url/labels/nodeKeys validation,
 * Bolt/Neo4j URL scheme warning, writeTarget ('labels' | 'relationship' | 'query') switching,
 * batch.size configuration, SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { Neo4jIOSinkProperties, Neo4jSinkProperties } from './Neo4jIO';

describe('Neo4jIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports Neo4jSinkProperties alias and validates required url, labels, and nodeKeys in default mode', () => {
    expect(Neo4jSinkProperties).toBe(Neo4jIOSinkProperties);
    render(<Neo4jIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Connection URL is required')).toBeInTheDocument();
    expect(screen.getByText('Node labels are required in Labels mode')).toBeInTheDocument();
    expect(screen.getByText('Node keys are required for Overwrite (MERGE) mode')).toBeInTheDocument();
    expect(screen.getByDisplayValue('5000')).toBeInTheDocument();
  });

  it('warns on invalid URL scheme and hydrates labels + nodeKeys properties', () => {
    const { container } = render(
      <Neo4jIOSinkProperties
        node={{
          data: {
            properties: {
              url: 'http://wrong-scheme:7687',
              database: 'graph_prod',
              saveMode: 'Overwrite',
              writeTarget: 'labels',
              labels: ':Customer:VIP',
              nodeKeys: 'customer_id',
              batchSize: 2000,
              username: 'neo4j_writer',
              authMode: 'secretManager',
              secretName: 'neo4j-bolt-pass',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText('Connection URL must start with neo4j://, neo4j+s://, bolt://, or bolt+s://')
    ).toBeInTheDocument();
    expect(screen.getByDisplayValue('graph_prod')).toBeInTheDocument();
    expect(screen.getByDisplayValue(':Customer:VIP')).toBeInTheDocument();
    expect(screen.getByDisplayValue('customer_id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('2000')).toBeInTheDocument();
    expect(screen.getByDisplayValue('neo4j_writer')).toBeInTheDocument();
    expect(screen.getByDisplayValue('neo4j-bolt-pass')).toBeInTheDocument();
    expect(container.querySelector('#neo4jSinkSaveMode')).toHaveValue('Overwrite');
  });

  it('switches across relationship and custom Cypher query write targets and dispatches updates', () => {
    const { container, rerender } = render(
      <Neo4jIOSinkProperties
        node={{
          data: {
            properties: {
              url: 'neo4j+s://aura.databases.neo4j.io',
              writeTarget: 'relationship',
              relationship: 'PURCHASED',
              sourceLabels: ':Customer',
              sourceNodeKeys: 'customer_id:id',
              targetLabels: ':Product',
              targetNodeKeys: 'product_id:id',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByDisplayValue('PURCHASED')).toBeInTheDocument();
    expect(screen.getByDisplayValue('customer_id:id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('product_id:id')).toBeInTheDocument();

    const targetSelect = container.querySelector('#neo4jSinkWriteTarget') as HTMLSelectElement;
    fireEvent.change(targetSelect, { target: { name: 'writeTarget', value: 'query' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ writeTarget: 'query' }));

    rerender(
      <Neo4jIOSinkProperties
        node={{
          data: {
            properties: {
              url: 'neo4j+s://aura.databases.neo4j.io',
              writeTarget: 'query',
              query: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText('Cypher query is required in Query mode')).toBeInTheDocument();

    const queryInput = container.querySelector('#neo4jSinkQuery') as HTMLTextAreaElement;
    fireEvent.change(queryInput, {
      target: { name: 'query', value: 'MERGE (n:Account {id: event.id})' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ query: 'MERGE (n:Account {id: event.id})' })
    );

    const sysInput = container.querySelector('#neo4jSinkInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Repartition by source node key' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Repartition by source node key' })
    );
  });
});
