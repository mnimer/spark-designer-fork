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
  it('normalizes unknown and unset runner identifiers to Local using the allowlist', () => {
    expect(normalizeSparkRunner(undefined)).toBe('Local');
    expect(normalizeSparkRunner('')).toBe('Local');
    expect(normalizeSparkRunner('LegacyRunner')).toBe('Local');
    expect(normalizeSparkRunner('Spark')).toBe('Local');
    expect(normalizeSparkRunner('Local')).toBe('Local');
    expect(normalizeSparkRunner('DataprocCluster')).toBe('DataprocCluster');
    expect(normalizeSparkRunner('ServerlessSpark')).toBe('ServerlessSpark');
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

  it('defaults to Local when runner is empty or outside the Spark allowlist', () => {
    const { rerender } = render(<RunnerSettings runner="" setJobConfig={vi.fn()} />);
    expect(screen.getByLabelText(/Target Environment/i)).toHaveValue('Local');

    rerender(<RunnerSettings runner="LegacyRunner" setJobConfig={vi.fn()} />);
    expect(screen.getByLabelText(/Target Environment/i)).toHaveValue('Local');

    rerender(<RunnerSettings runner="Spark" setJobConfig={vi.fn()} />);
    expect(screen.getByLabelText(/Target Environment/i)).toHaveValue('Local');
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

  it('aligns Spark 4.2+ runtimeVersion via functional updater when paradigm is sdp (SDP3)', () => {
    let state: Record<string, unknown> = {
      runner: 'ServerlessSpark',
      runtimeVersion: '3.0',
      projectId: 'proj-sdp',
    };
    const setJobConfig = vi.fn((updater) => {
      state = typeof updater === 'function' ? updater(state) : updater;
    });

    const { rerender } = render(
      <RunnerSettings runner="ServerlessSpark" setJobConfig={setJobConfig} paradigm="sdp" />
    );

    fireEvent.change(screen.getByLabelText(/Target Environment/i), {
      target: { value: 'DataprocCluster' },
    });
    expect(state).toEqual({
      runner: 'DataprocCluster',
      runtimeVersion: '4.0',
      projectId: 'proj-sdp',
    });

    rerender(
      <RunnerSettings runner="DataprocCluster" setJobConfig={setJobConfig} paradigm="sdp" />
    );
    fireEvent.change(screen.getByLabelText(/Target Environment/i), {
      target: { value: 'Local' },
    });
    expect(state).toEqual({
      runner: 'Local',
      runtimeVersion: '4.2-connect',
      useSparkConnect: true,
      projectId: 'proj-sdp',
    });
  });
});
