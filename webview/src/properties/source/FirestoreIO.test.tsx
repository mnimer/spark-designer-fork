/**
 * Unit Tests for FirestoreIOSourceProperties Component (SRC33)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates FirestoreIOSourceProperties rendering, projectId/databaseId/collectionPath
 * validation, filterJson, SchemaEditor integration, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { FirestoreIOSourceProperties, FirestoreSourceProperties } from './FirestoreIO';

describe('FirestoreIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports FirestoreSourceProperties alias and validates required projectId and collectionPath', () => {
    expect(FirestoreSourceProperties).toBe(FirestoreIOSourceProperties);
    render(<FirestoreIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Project ID is required')).toBeInTheDocument();
    expect(screen.getByText('Collection path is required')).toBeInTheDocument();
    expect(screen.getByDisplayValue('(default)')).toBeInTheDocument();
  });

  it('hydrates projectId, databaseId, collectionPath, filterJson, and SchemaEditor fields', () => {
    const node = {
      data: {
        properties: {
          projectId: 'my-firestore-proj',
          databaseId: '(default)',
          collectionPath: 'orders',
          filterJson: '[{"field":"status","op":"==","value":"paid"}]',
          schemaString: 'order_id:STRING:REQUIRED, amount:FLOAT',
        },
      },
    };

    const { container } = render(<FirestoreIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByDisplayValue('my-firestore-proj')).toBeInTheDocument();
    expect(screen.getByDisplayValue('orders')).toBeInTheDocument();
    expect(screen.getByDisplayValue('[{"field":"status","op":"==","value":"paid"}]')).toBeInTheDocument();
    expect(screen.getByText('Field Builder (2)')).toBeInTheDocument();

    const collInput = container.querySelector('#firestoreSourceCollectionPath') as HTMLInputElement;
    fireEvent.change(collInput, { target: { name: 'collectionPath', value: 'customers' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ collectionPath: 'customers' })
    );

    const sysInput = container.querySelector('#firestoreSourceInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Include document ID column' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Include document ID column' })
    );
  });
});
