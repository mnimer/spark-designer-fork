/**
 * @fileoverview Unit tests for the Spark ConditionalSplit step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  ConditionalSplitProperties,
  normalizeConditionalSplitRules,
  buildNextUniqueRuleName,
  validateConditionalSplitRules,
} from './ConditionalSplit';

describe('ConditionalSplitProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-split-1',
    data: {
      label: 'Conditional Split',
      spark_transform: 'ConditionalSplit',
      properties,
    },
  });

  describe('rule normalization and validation helpers', () => {
    it('defaults rules to [{ name: rule_1, condition: "" }] matching getConditionalSplitHandles', () => {
      expect(normalizeConditionalSplitRules(undefined)).toEqual([
        { name: 'rule_1', condition: '' },
      ]);
      expect(normalizeConditionalSplitRules([])).toEqual([{ name: 'rule_1', condition: '' }]);
    });

    it('generates non-colliding unique rule names skipping existing names', () => {
      expect(
        buildNextUniqueRuleName([
          { name: 'rule_1', condition: 'a > 1' },
          { name: 'rule_2', condition: 'a > 2' },
        ])
      ).toBe('rule_3');

      expect(
        buildNextUniqueRuleName([
          { name: 'vip', condition: 'a > 10' },
          { name: 'rule_3', condition: 'a > 0' },
        ])
      ).toBe('rule_4');
    });

    it('rejects reserved else rule names, duplicate rule names, and empty conditions', () => {
      const errors = validateConditionalSplitRules([
        { name: 'else', condition: 'amount > 0' },
        { name: 'vip', condition: 'amount > 1000' },
        { name: 'vip', condition: '' },
      ]);

      expect(errors[0].nameError).toMatch(/reserved/i);
      expect(errors[0].conditionError).toBeNull();
      expect(errors[1].nameError).toMatch(/Duplicate rule name "vip"/i);
      expect(errors[2].nameError).toMatch(/Duplicate rule name "vip"/i);
      expect(errors[2].conditionError).toMatch(/condition is required/i);
    });
  });

  it('renders the read-only else fallback handle preview card and edits evaluationMode', () => {
    const handleChange = vi.fn();
    render(
      <ConditionalSplitProperties
        node={createNode({
          rules: [{ name: 'vip', condition: 'amount >= 1000' }],
          evaluationMode: 'first_match',
        })}
        onChange={handleChange}
      />
    );

    const elseCard = screen.getByTestId('conditional-split-else-preview');
    expect(elseCard).toHaveTextContent(/Fallback Handle/i);
    expect(elseCard).toHaveTextContent('else');

    const modeSelect = screen.getByLabelText(/Evaluation Mode/i, { selector: 'select' });
    fireEvent.change(modeSelect, { target: { value: 'independent' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        evaluationMode: 'independent',
        rules: [{ name: 'vip', condition: 'amount >= 1000' }],
      })
    );
  });

  it('flags naming a rule else or duplicating an existing rule name', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <ConditionalSplitProperties
        node={createNode({
          rules: [{ name: 'else', condition: 'amount > 10' }],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByRole('alert')).toHaveTextContent(/reserved/i);
    expect(screen.getByLabelText('Rule 1 name')).toHaveAttribute('aria-invalid', 'true');

    rerender(
      <ConditionalSplitProperties
        node={createNode({
          rules: [
            { name: 'fraud', condition: 'risk_score > 90' },
            { name: 'fraud', condition: 'chargeback = true' },
          ],
        })}
        onChange={handleChange}
      />
    );

    const alerts = screen.getAllByRole('alert');
    expect(alerts).toHaveLength(2);
    expect(alerts[0]).toHaveTextContent(/Duplicate rule name "fraud"/i);
  });

  it('adds unique rules, reorders waterfall rules, removes rules, and updates system instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <ConditionalSplitProperties
        node={createNode({
          rules: [{ name: 'rule_1', condition: 'amount >= 1000' }],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Rule/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        rules: [
          { name: 'rule_1', condition: 'amount >= 1000' },
          { name: 'rule_2', condition: '' },
        ],
      })
    );

    handleChange.mockClear();
    rerender(
      <ConditionalSplitProperties
        node={createNode({
          rules: [
            { name: 'vip', condition: 'amount >= 1000' },
            { name: 'standard', condition: 'amount >= 100' },
          ],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'Move rule 2 up' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        rules: [
          { name: 'standard', condition: 'amount >= 100' },
          { name: 'vip', condition: 'amount >= 1000' },
        ],
      })
    );

    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Remove rule 2' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        rules: [{ name: 'vip', condition: 'amount >= 1000' }],
      })
    );

    const instructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructions, { target: { value: 'Route fraud branch to Kafka DLQ' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Route fraud branch to Kafka DLQ',
      })
    );
  });
});
