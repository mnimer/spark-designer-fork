/**
 * @fileoverview Unit tests for AdvancedConfig pipeline-level streaming defaults and runner options.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { AdvancedConfig, type AdvancedJobConfig } from './AdvancedConfig';

describe('AdvancedConfig', () => {
  const sampleSubnetworks = [
    { name: 'spark-subnet', region: 'us-central1', network: 'default', value: 'spark-subnet' },
  ];

  it('renders Streaming Defaults with processingTime and 10 seconds interval by default', () => {
    const jobConfig: AdvancedJobConfig = { runner: 'Spark' };
    render(
      <AdvancedConfig
        jobConfig={jobConfig}
        setJobConfig={vi.fn()}
        subnetworks={sampleSubnetworks}
      />
    );

    expect(screen.getByTestId('pipeline-streaming-defaults')).toBeInTheDocument();
    expect(screen.getByLabelText(/Checkpoint Root URI/i)).toHaveValue('');
    expect(screen.getByLabelText(/Default Trigger Type/i)).toHaveValue('processingTime');
    expect(screen.getByLabelText(/Default Trigger Interval/i)).toHaveValue('10 seconds');
  });

  it('updates checkpointRoot, defaultTriggerType, and defaultTriggerInterval via functional setJobConfig', () => {
    let state: AdvancedJobConfig = { runner: 'ServerlessSpark', region: 'us-central1' };
    const setJobConfig = vi.fn((updater) => {
      state = typeof updater === 'function' ? updater(state) : updater;
    });

    const { rerender } = render(
      <AdvancedConfig
        jobConfig={state}
        setJobConfig={setJobConfig}
        subnetworks={sampleSubnetworks}
      />
    );

    fireEvent.change(screen.getByLabelText(/Checkpoint Root URI/i), {
      target: { value: 'gs://my-bucket/checkpoints' },
    });
    expect(state.checkpointRoot).toBe('gs://my-bucket/checkpoints');
    expect(state.region).toBe('us-central1');

    rerender(
      <AdvancedConfig
        jobConfig={state}
        setJobConfig={setJobConfig}
        subnetworks={sampleSubnetworks}
      />
    );

    fireEvent.change(screen.getByLabelText(/Default Trigger Interval/i), {
      target: { value: '30 seconds' },
    });
    expect(state.defaultTriggerInterval).toBe('30 seconds');

    rerender(
      <AdvancedConfig
        jobConfig={state}
        setJobConfig={setJobConfig}
        subnetworks={sampleSubnetworks}
      />
    );

    // Switching to availableNow hides Default Trigger Interval
    fireEvent.change(screen.getByLabelText(/Default Trigger Type/i), {
      target: { value: 'availableNow' },
    });
    expect(state.defaultTriggerType).toBe('availableNow');

    rerender(
      <AdvancedConfig
        jobConfig={state}
        setJobConfig={setJobConfig}
        subnetworks={sampleSubnetworks}
      />
    );
    expect(screen.queryByLabelText(/Default Trigger Interval/i)).not.toBeInTheDocument();

    // Switching to once also hides Default Trigger Interval
    fireEvent.change(screen.getByLabelText(/Default Trigger Type/i), {
      target: { value: 'once' },
    });
    rerender(
      <AdvancedConfig
        jobConfig={state}
        setJobConfig={setJobConfig}
        subnetworks={sampleSubnetworks}
      />
    );
    expect(screen.queryByLabelText(/Default Trigger Interval/i)).not.toBeInTheDocument();

    // Switching to continuous shows Default Trigger Interval
    fireEvent.change(screen.getByLabelText(/Default Trigger Type/i), {
      target: { value: 'continuous' },
    });
    rerender(
      <AdvancedConfig
        jobConfig={state}
        setJobConfig={setJobConfig}
        subnetworks={sampleSubnetworks}
      />
    );
    expect(screen.getByLabelText(/Default Trigger Interval/i)).toHaveValue('30 seconds');
  });
});
