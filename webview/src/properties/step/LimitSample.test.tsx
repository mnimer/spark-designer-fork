/**
 * @fileoverview Unit tests for the Spark LimitSample step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  LimitSampleProperties,
  validateLimitCount,
  validateSampleFraction,
  validateSampleSeed,
} from './LimitSample';

describe('LimitSampleProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-limitsample-1',
    data: {
      label: 'Limit / Sample',
      spark_transform: 'LimitSample',
      properties,
    },
  });

  describe('validation helpers', () => {
    it('validates positive integer limitCount boundaries', () => {
      expect(validateLimitCount(1)).toBeNull();
      expect(validateLimitCount(1000)).toBeNull();
      expect(validateLimitCount('250')).toBeNull();
      expect(validateLimitCount(0)).toMatch(/positive integer/i);
      expect(validateLimitCount(-10)).toMatch(/positive integer/i);
      expect(validateLimitCount('2.5')).toMatch(/positive integer/i);
      expect(validateLimitCount('')).toMatch(/required/i);
    });

    it('validates sample fraction in [0, 1] and optional integer seed', () => {
      expect(validateSampleFraction(0)).toBeNull();
      expect(validateSampleFraction(0.1)).toBeNull();
      expect(validateSampleFraction(1)).toBeNull();
      expect(validateSampleFraction(-0.01)).toMatch(/between 0\.0 and 1\.0/i);
      expect(validateSampleFraction(1.2)).toMatch(/between 0\.0 and 1\.0/i);
      expect(validateSampleFraction('')).toMatch(/required/i);

      expect(validateSampleSeed(undefined)).toBeNull();
      expect(validateSampleSeed('')).toBeNull();
      expect(validateSampleSeed(42)).toBeNull();
      expect(validateSampleSeed(-7)).toBeNull();
      expect(validateSampleSeed('3.14')).toMatch(/integer/i);
    });
  });

  it('renders default limit mode with 1000 rows and flags negative limit counts', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <LimitSampleProperties node={createNode()} onChange={handleChange} />
    );

    const limitInput = screen.getByLabelText(/Row Limit/i, { selector: 'input' });
    expect(limitInput).toHaveValue(1000);
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();

    fireEvent.change(limitInput, { target: { value: '250' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'limit',
        limitCount: 250,
      })
    );

    rerender(
      <LimitSampleProperties
        node={createNode({ mode: 'limit', limitCount: -5 })}
        onChange={handleChange}
      />
    );
    expect(screen.getByRole('alert')).toHaveTextContent(/positive integer/i);
  });

  it('switches to sample mode, validates out-of-range fraction, and updates sample fields', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <LimitSampleProperties node={createNode()} onChange={handleChange} />
    );

    const modeSelect = screen.getByLabelText(/Execution Mode/i, { selector: 'select' });
    fireEvent.change(modeSelect, { target: { value: 'sample' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'sample',
        fraction: 0.1,
        withReplacement: false,
      })
    );

    handleChange.mockClear();
    rerender(
      <LimitSampleProperties
        node={createNode({ mode: 'sample', fraction: 1.5, withReplacement: false })}
        onChange={handleChange}
      />
    );

    expect(screen.getByRole('alert')).toHaveTextContent(/between 0\.0 and 1\.0/i);

    const fractionInput = screen.getByLabelText(/Sample Fraction/i, { selector: 'input' });
    fireEvent.change(fractionInput, { target: { value: '0.25' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'sample',
        fraction: 0.25,
      })
    );

    const replacementCheckbox = screen.getByLabelText(/Sample With Replacement/i, {
      selector: 'input',
    });
    fireEvent.click(replacementCheckbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        withReplacement: true,
      })
    );

    const seedInput = screen.getByLabelText(/Random Seed/i, { selector: 'input' });
    fireEvent.change(seedInput, { target: { value: '42' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        seed: 42,
      })
    );

    const instructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructions, { target: { value: 'Downsample staging branch' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Downsample staging branch',
      })
    );
  });
});
