/**
 * @fileoverview Unit tests for RunnerSettings component (R1).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { RunnerSettings, normalizeSparkRunner } from './RunnerSettings';

describe('RunnerSettings (R1)', () => {
  it('normalizes legacy and unset runner identifiers cleanly', () => {
    expect(normalizeSparkRunner(undefined)).toBe('Local');
    expect(normalizeSparkRunner('')).toBe('Local');
    expect(normalizeSparkRunner('DirectRunner')).toBe('Local');
    expect(normalizeSparkRunner('Local')).toBe('Local');
    expect(normalizeSparkRunner('DataprocCluster')).toBe('DataprocCluster');
    expect(normalizeSparkRunner('ServerlessSpark')).toBe('ServerlessSpark');
    expect(normalizeSparkRunner('Spark')).toBe('ServerlessSpark');
  });

  it('renders the 3 Spark target environments with exact labels', () => {
    render(<RunnerSettings runner="Local" setJobConfig={vi.fn()} />);

    const select = screen.getByLabelText(/Target Environment/i) as HTMLSelectElement;
    expect(select).toBeInTheDocument();
    expect(select.value).toBe('Local');

    const options = Array.from(select.options).map((o) => ({
      value: o.value,
      label: o.textContent,
    }));

    expect(options).toEqual([
      { value: 'Local', label: 'Local PySpark / Spark Connect' },
      { value: 'DataprocCluster', label: 'Dataproc Cluster (GCE/GKE)' },
      { value: 'ServerlessSpark', label: 'GCP Serverless Spark (Batch)' },
    ]);
  });

  it('defaults to Local when runner is empty or legacy DirectRunner, and maps legacy Spark to ServerlessSpark', () => {
    const { rerender } = render(<RunnerSettings runner="" setJobConfig={vi.fn()} />);
    expect(screen.getByLabelText(/Target Environment/i)).toHaveValue('Local');

    rerender(<RunnerSettings runner="DirectRunner" setJobConfig={vi.fn()} />);
    expect(screen.getByLabelText(/Target Environment/i)).toHaveValue('Local');

    rerender(<RunnerSettings runner="Spark" setJobConfig={vi.fn()} />);
    expect(screen.getByLabelText(/Target Environment/i)).toHaveValue('ServerlessSpark');
  });

  it('updates runner via functional setJobConfig updater', () => {
    let state: Record<string, unknown> = { runner: 'Local', region: 'us-central1' };
    const setJobConfig = vi.fn((updater) => {
      state = typeof updater === 'function' ? updater(state) : updater;
    });

    render(<RunnerSettings runner="Local" setJobConfig={setJobConfig} />);

    fireEvent.change(screen.getByLabelText(/Target Environment/i), {
      target: { value: 'DataprocCluster' },
    });

    expect(setJobConfig).toHaveBeenCalledTimes(1);
    expect(typeof setJobConfig.mock.calls[0][0]).toBe('function');
    expect(state).toEqual({
      runner: 'DataprocCluster',
      region: 'us-central1',
    });
  });
});
