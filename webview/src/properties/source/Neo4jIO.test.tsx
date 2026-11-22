/**
 * Unit Tests for Neo4jIOSourceProperties Component (SRC37)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Neo4jIOSourceProperties rendering, readMode ('batch' | 'streaming'),
 * url/database, queryMode ('labels' | 'relationship' | 'query'), streamingFrom and
 * streamingPropertyName when streaming, SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { Neo4jIOSourceProperties, Neo4jSourceProperties } from './Neo4jIO';

describe('Neo4jIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports Neo4jSourceProperties alias and validates required url and labels in default mode', () => {
    expect(Neo4jSourceProperties).toBe(Neo4jIOSourceProperties);
    render(<Neo4jIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Connection URL is required')).toBeInTheDocument();
    expect(screen.getByText('Node labels are required in Labels mode')).toBeInTheDocument();
    expect(screen.getByDisplayValue('neo4j')).toBeInTheDocument();
  });

  it('switches across queryMode (labels, relationship, query) and dispatches updates', () => {
    const { container, rerender } = render(
      <Neo4jIOSourceProperties
        node={{
          data: {
            properties: {
              url: 'bolt://localhost:7687',
              queryMode: 'labels',
              labels: ':Customer',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByDisplayValue(':Customer')).toBeInTheDocument();

    rerender(
      <Neo4jIOSourceProperties
        node={{
          data: {
            properties: {
              url: 'bolt://localhost:7687',
              queryMode: 'relationship',
              relationship: 'PURCHASED',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByDisplayValue('PURCHASED')).toBeInTheDocument();

    rerender(
      <Neo4jIOSourceProperties
        node={{
          data: {
            properties: {
              url: 'bolt://localhost:7687',
              queryMode: 'query',
              query: 'MATCH (n:Person) RETURN n.id AS id',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const queryInput = container.querySelector('#neo4jSourceQuery') as HTMLTextAreaElement;
    expect(queryInput).toHaveValue('MATCH (n:Person) RETURN n.id AS id');
    fireEvent.change(queryInput, { target: { name: 'query', value: 'MATCH (n) RETURN n' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ query: 'MATCH (n) RETURN n' })
    );
  });

  it('renders streamingFrom and streamingPropertyName when readMode is streaming', () => {
    const { container } = render(
      <Neo4jIOSourceProperties
        node={{
          data: {
            properties: {
              readMode: 'streaming',
              url: 'neo4j+s://aura.databases.neo4j.io',
              queryMode: 'labels',
              labels: ':Event',
              streamingFrom: 'ALL',
              streamingPropertyName: 'updated_at',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const fromSelect = container.querySelector('#neo4jSourceStreamingFrom') as HTMLSelectElement;
    const propInput = container.querySelector('#neo4jSourceStreamingPropertyName') as HTMLInputElement;

    expect(fromSelect).toHaveValue('ALL');
    expect(propInput).toHaveValue('updated_at');

    fireEvent.change(fromSelect, { target: { name: 'streamingFrom', value: 'NOW' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ streamingFrom: 'NOW' }));

    const sysInput = container.querySelector('#neo4jSourceInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Stream graph updates' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Stream graph updates' })
    );
  });
});
