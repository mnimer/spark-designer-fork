/**
 * @fileoverview Unit tests for the Spark Join step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  JoinProperties,
  normalizeJoinKeys,
  normalizeJoinTypeValue,
} from './Join';

describe('JoinProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-join-1',
    data: {
      label: 'Join',
      spark_transform: 'Join',
      properties,
    },
  });

  describe('normalization helpers', () => {
    it('normalizes canonical and legacy joinType spellings cleanly', () => {
      expect(normalizeJoinTypeValue(undefined)).toBe('inner');
      expect(normalizeJoinTypeValue('left')).toBe('left_outer');
      expect(normalizeJoinTypeValue('leftOuter')).toBe('left_outer');
      expect(normalizeJoinTypeValue('right')).toBe('right_outer');
      expect(normalizeJoinTypeValue('full')).toBe('full_outer');
      expect(normalizeJoinTypeValue('outer')).toBe('full_outer');
      expect(normalizeJoinTypeValue('leftSemi')).toBe('left_semi');
      expect(normalizeJoinTypeValue('anti')).toBe('left_anti');
      expect(normalizeJoinTypeValue('cross')).toBe('cross');
      expect(normalizeJoinTypeValue('unknown_mode')).toBe('inner');
    });

    it('provides a default empty key pair when joinKeys is missing or empty', () => {
      expect(normalizeJoinKeys(undefined)).toEqual([{ leftColumn: '', rightColumn: '' }]);
      expect(normalizeJoinKeys([])).toEqual([{ leftColumn: '', rightColumn: '' }]);
      expect(normalizeJoinKeys([{ leftColumn: 'id', rightColumn: 'user_id' }])).toEqual([
        { leftColumn: 'id', rightColumn: 'user_id' },
      ]);
    });
  });

  it('renders all 7 Spark join types, supports legacy how fallback, and toggles broadcastRight', () => {
    const handleChange = vi.fn();
    render(
      <JoinProperties
        node={createNode({ how: 'left_outer', broadcastRight: false })}
        onChange={handleChange}
      />
    );

    const joinTypeSelect = screen.getByLabelText(/Join Type/i, {
      selector: 'select',
    }) as HTMLSelectElement;
    expect(joinTypeSelect.value).toBe('left_outer');
    expect(Array.from(joinTypeSelect.options).map((o) => o.value)).toEqual([
      'inner',
      'left_outer',
      'right_outer',
      'full_outer',
      'left_semi',
      'left_anti',
      'cross',
    ]);

    fireEvent.change(joinTypeSelect, { target: { value: 'full_outer' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        joinType: 'full_outer',
      })
    );

    const broadcastCheckbox = screen.getByLabelText(/Broadcast Right Input/i, {
      selector: 'input',
    });
    expect(broadcastCheckbox).not.toBeChecked();
    fireEvent.click(broadcastCheckbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        broadcastRight: true,
      })
    );
  });

  it('adds, edits, and removes equi-join key pairs when conditionMode is keys', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <JoinProperties
        node={createNode({
          joinType: 'inner',
          conditionMode: 'keys',
          joinKeys: [{ leftColumn: 'order_cust_id', rightColumn: 'cust_id' }],
        })}
        onChange={handleChange}
      />
    );

    const leftInput = screen.getByLabelText('Join key 1 left column');
    expect(leftInput).toHaveValue('order_cust_id');
    fireEvent.change(leftInput, { target: { value: 'customer_id' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        joinKeys: [{ leftColumn: 'customer_id', rightColumn: 'cust_id' }],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Key/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        joinKeys: [
          { leftColumn: 'order_cust_id', rightColumn: 'cust_id' },
          { leftColumn: '', rightColumn: '' },
        ],
      })
    );

    handleChange.mockClear();
    rerender(
      <JoinProperties
        node={createNode({
          joinType: 'inner',
          conditionMode: 'keys',
          joinKeys: [
            { leftColumn: 'customer_id', rightColumn: 'customer_id' },
            { leftColumn: 'tenant_id', rightColumn: 'tenant_id' },
          ],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove join key 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        joinKeys: [{ leftColumn: 'tenant_id', rightColumn: 'tenant_id' }],
      })
    );
  });

  it('validates custom expression mode and hides condition editor for cross joins', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <JoinProperties
        node={createNode({
          joinType: 'inner',
          conditionMode: 'expression',
          joinExpression: '',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByRole('alert')).toHaveTextContent('Join condition expression is required.');

    const exprTextarea = screen.getByLabelText(/Join Condition Expression/i, {
      selector: 'textarea',
    });
    fireEvent.change(exprTextarea, { target: { name: 'joinExpression', value: 'l.id = r.id' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        joinExpression: 'l.id = r.id',
      })
    );

    rerender(
      <JoinProperties node={createNode({ joinType: 'cross' })} onChange={handleChange} />
    );
    expect(screen.queryByTestId('join-condition-section')).not.toBeInTheDocument();
  });

  it('updates streamTimeConstraint, dropDuplicateRightKeys, rightColumnPrefix, and hides right disambiguation on left_semi', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <JoinProperties node={createNode({ joinType: 'left_outer' })} onChange={handleChange} />
    );

    const streamConstraintInput = screen.getByLabelText(/Stream-Stream Time Constraint/i, {
      selector: 'input',
    });
    fireEvent.change(streamConstraintInput, {
      target: {
        name: 'streamTimeConstraint',
        value: 'r.ts >= l.ts AND r.ts <= l.ts + INTERVAL 5 MINUTES',
      },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        streamTimeConstraint: 'r.ts >= l.ts AND r.ts <= l.ts + INTERVAL 5 MINUTES',
      })
    );

    const dropDupCheckbox = screen.getByLabelText(/Drop Duplicate Right Join Keys/i, {
      selector: 'input',
    });
    expect(dropDupCheckbox).toBeChecked();
    fireEvent.click(dropDupCheckbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dropDuplicateRightKeys: false,
      })
    );

    const prefixInput = screen.getByLabelText(/Right Column Disambiguation Prefix/i, {
      selector: 'input',
    });
    fireEvent.change(prefixInput, {
      target: { name: 'rightColumnPrefix', value: 'dim_' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        rightColumnPrefix: 'dim_',
      })
    );

    const sysTextarea = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(sysTextarea, { target: { value: 'Coalesce outer join keys' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Coalesce outer join keys',
      })
    );

    rerender(
      <JoinProperties node={createNode({ joinType: 'left_semi' })} onChange={handleChange} />
    );
    expect(
      screen.queryByLabelText(/Drop Duplicate Right Join Keys/i, { selector: 'input' })
    ).not.toBeInTheDocument();
    expect(
      screen.queryByLabelText(/Right Column Disambiguation Prefix/i, { selector: 'input' })
    ).not.toBeInTheDocument();
  });
});
