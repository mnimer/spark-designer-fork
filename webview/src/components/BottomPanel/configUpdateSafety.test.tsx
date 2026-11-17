/**
 * Regression tests for config updates clobbering sibling fields.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Ensures child controls always pass an updater function `(prev) => ({ ...prev, ... })`
 * to `setJobConfig` and `setPipelineConfig` so sibling fields modified after the child's
 * render snapshot are never overwritten.
 */

import { describe, it, expect, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { DeploymentTab } from './DeploymentTab';
import { SourceCodeTab } from './SourceCodeTab';
import { renderWithBottomPanel } from './testUtils';

/* eslint-disable @typescript-eslint/no-explicit-any */

describe('config updates never clobber sibling fields (regression)', () => {
  const jobConfig = {
    runner: 'ServerlessSpark',
    runtimeVersion: '4.0',
    tier: 'standard',
    depsBucket: 'gs://my-bucket',
    gcsStagingPath: 'gs://my-bucket',
    network: '',
    subnetwork: 'regions/us-central1/subnetworks/default',
  };

  const deploymentProps = {
    jobConfig,
    runtimeVariables: [{ key: 'JOB_ID', value: 'j' }],
    addVariable: vi.fn(),
    updateVariable: vi.fn(),
    deleteVariable: vi.fn(),
    isJobIdValid: () => true,
    onRunNow: vi.fn(),
    subnetworks: [],
  };

  it('runner change sends an updater, not a snapshot-derived object', () => {
    const setJobConfig = vi.fn();
    renderWithBottomPanel(<DeploymentTab />, { ...deploymentProps, setJobConfig });

    const runner = screen.getByLabelText(/Target Environment/i);
    fireEvent.change(runner, { target: { value: 'Local' } });

    expect(setJobConfig).toHaveBeenCalledTimes(1);
    const arg = setJobConfig.mock.calls[0][0];
    expect(
      typeof arg,
      'setJobConfig must receive an updater function so the merge uses the freshest state'
    ).toBe('function');
  });

  it('the runner updater preserves every other job config field', () => {
    const setJobConfig = vi.fn();
    renderWithBottomPanel(<DeploymentTab />, { ...deploymentProps, setJobConfig });

    fireEvent.change(screen.getByLabelText(/Target Environment/i), {
      target: { value: 'DataprocCluster' },
    });

    const updater = setJobConfig.mock.calls[0][0] as (p: any) => any;

    const fresher = {
      ...jobConfig,
      depsBucket: 'gs://changed-after-render',
      runtimeVersion: '3.0',
    };
    const next = updater(fresher);

    expect(next.runner).toBe('DataprocCluster');
    expect(next.depsBucket).toBe('gs://changed-after-render');
    expect(next.runtimeVersion).toBe('3.0');
    expect(next.subnetwork).toBe(jobConfig.subnetwork);
  });

  it('outputFolder change sends an updater that preserves the rest of pipelineConfig', () => {
    const setPipelineConfig = vi.fn();
    renderWithBottomPanel(
      <SourceCodeTab />,
      {
        language: 'Python',
        outputFolder: 'out',
        setPipelineConfig,
      }
    );

    const input = screen.getByPlaceholderText('./scratch_flow/');
    fireEvent.change(input, { target: { value: 'new_out' } });

    expect(setPipelineConfig).toHaveBeenCalledTimes(1);
    const updater = setPipelineConfig.mock.calls[0][0] as (p: any) => any;
    expect(typeof updater).toBe('function');

    const fresher = { language: 'Python' as const, outputFolder: 'out', needsRebuild: false };
    const next = updater(fresher);
    expect(next.outputFolder).toBe('new_out');
    expect(next.language).toBe('Python');
    expect(next.needsRebuild).toBe(true);
  });
});
