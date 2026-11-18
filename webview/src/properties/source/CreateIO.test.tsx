/**
 * @fileoverview Unit tests for CreateProperties / CreateDataFrameSourceProperties (SRC38).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates all three synthetic generation modes ('rows', 'range', 'faker'),
 * dynamic element addition and removal, JSON string normalization, SchemaEditor
 * integration, and SystemInstructionsField updates.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  CreateProperties,
  CreateDataFrameSourceProperties,
  normalizeElements,
} from './CreateIO';

describe('CreateProperties (SRC38 CreateDataFrameSource)', () => {
  it('exports CreateDataFrameSourceProperties as an alias for CreateProperties', () => {
    expect(CreateDataFrameSourceProperties).toBe(CreateProperties);
  });

  it('normalizes arrays, JSON strings, objects, and primitives via normalizeElements', () => {
    expect(normalizeElements(undefined)).toEqual(['']);
    expect(normalizeElements([])).toEqual(['']);
    expect(normalizeElements([{ id: 1 }, 42])).toEqual(['{"id":1}', '42']);
    expect(normalizeElements('["a", "b"]')).toEqual(['a', 'b']);
    expect(normalizeElements('single')).toEqual(['single']);
  });

  it('renders rows mode by default and supports adding, editing, and removing elements', () => {
    const mockNode = {
      data: {
        properties: {
          elements: ['{"id": 1}', '{"id": 2}'],
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<CreateProperties node={mockNode} onChange={mockOnChange} />);

    const textareas = screen.getAllByPlaceholderText(/Element \d/);
    expect(textareas).toHaveLength(2);
    expect(textareas[0]).toHaveValue('{"id": 1}');
    expect(textareas[1]).toHaveValue('{"id": 2}');

    fireEvent.change(textareas[0], { target: { value: '{"id": 10}' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        elements: ['{"id": 10}', '{"id": 2}'],
        rows: ['{"id": 10}', '{"id": 2}'],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: /add item/i }));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        elements: ['{"id": 1}', '{"id": 2}', ''],
      })
    );

    const deleteButtons = screen.getAllByTitle('Remove Element');
    fireEvent.click(deleteButtons[0]);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        elements: ['{"id": 2}'],
      })
    );
  });

  it('displays required validation error when all row elements are empty in rows mode', () => {
    const mockNode = {
      data: {
        properties: {
          generationMode: 'rows',
          elements: ['   '],
        },
      },
    };
    render(<CreateProperties node={mockNode} onChange={vi.fn()} />);

    expect(screen.getByText('At least one non-empty element is required.')).toBeInTheDocument();
  });

  it('renders range mode controls (rangeStart, rangeEnd, rangeStep, numPartitions) and propagates updates', () => {
    const mockNode = {
      data: {
        properties: {
          generationMode: 'range',
          rangeStart: 10,
          rangeEnd: 5000,
          rangeStep: 5,
          numPartitions: 8,
        },
      },
    };
    const mockOnChange = vi.fn();

    const { container } = render(<CreateProperties node={mockNode} onChange={mockOnChange} />);

    expect(container.querySelector('#createGenerationMode')).toHaveValue('range');
    expect(container.querySelector('#createRangeStart')).toHaveValue(10);
    expect(container.querySelector('#createRangeEnd')).toHaveValue(5000);
    expect(container.querySelector('#createRangeStep')).toHaveValue(5);
    expect(container.querySelector('#createNumPartitions')).toHaveValue(8);

    fireEvent.change(container.querySelector('#createRangeEnd')!, {
      target: { name: 'rangeEnd', value: '10000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ rangeEnd: '10000' })
    );
  });

  it('renders faker mode controls (rowCount, fakerSpec, SchemaEditor) and propagates updates', () => {
    const mockNode = {
      data: {
        properties: {
          generationMode: 'faker',
          rowCount: 250,
          fakerSpec: '{"email": "email"}',
          schemaFields: [{ name: 'email', type: 'STRING', mode: 'REQUIRED' }],
          system_instructions: 'Seed Faker for deterministic tests',
        },
      },
    };
    const mockOnChange = vi.fn();

    const { container } = render(<CreateProperties node={mockNode} onChange={mockOnChange} />);

    expect(container.querySelector('#createGenerationMode')).toHaveValue('faker');
    expect(container.querySelector('#createRowCount')).toHaveValue(250);
    expect(container.querySelector('#createFakerSpec')).toHaveValue('{"email": "email"}');
    expect(screen.getByLabelText('Field #1 Name')).toHaveValue('email');

    fireEvent.change(container.querySelector('#createRowCount')!, {
      target: { name: 'rowCount', value: '500' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ rowCount: '500' }));

    fireEvent.change(container.querySelector('#createFakerSpec')!, {
      target: { name: 'fakerSpec', value: '{"name": "name"}' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ fakerSpec: '{"name": "name"}' })
    );

    fireEvent.change(container.querySelector('#createSystemInstructions')!, {
      target: { value: 'Updated Faker instructions' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Updated Faker instructions' })
    );
  });
});
