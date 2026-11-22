/**
 * @fileoverview Unit tests for SdpRefreshControls component (SDP9).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  SdpRefreshControls,
  type SdpRefreshDatasetOption,
} from './SdpRefreshControls';

describe('SdpRefreshControls (SDP9)', () => {
  const sampleDatasets: SdpRefreshDatasetOption[] = [
    {
      nodeId: 'n-view',
      nodeLabel: 'Temp Parsed View',
      datasetName: 'parsed_events_view',
      kind: 'temporary_view',
      badge: 'VIEW',
      resetAllowed: true,
    },
    {
      nodeId: 'n-bronze',
      nodeLabel: 'Bronze Orders',
      datasetName: 'bronze_orders',
      kind: 'streaming_table',
      badge: 'TABLE',
      resetAllowed: false,
    },
    {
      nodeId: 'n-silver',
      nodeLabel: 'Silver Customers CDC',
      datasetName: 'silver_customers',
      kind: 'auto_cdc',
      badge: 'CDC',
      resetAllowed: true,
    },
    {
      nodeId: 'n-gold',
      nodeLabel: 'Gold Store Revenue',
      datasetName: 'gold_store_revenue',
      kind: 'materialized_view',
      badge: 'MV',
      resetAllowed: true,
    },
    {
      nodeId: 'n-sink',
      nodeLabel: 'Kafka Outbound Sink',
      datasetName: 'kafka_orders_sink',
      kind: 'external_sink',
      badge: 'SINK',
      resetAllowed: true,
    },
  ];

  it('builds refresh checkboxes ONLY from persisted graph datasets (excluding temporary_view) and uses functional updaters', () => {
    let state: Record<string, unknown> = {
      sdpCommand: 'run',
      sdpRefreshMode: 'refresh',
      sdpRefreshDatasets: [],
      sdpFullRefreshDatasets: ['silver_customers'],
    };
    const setJobConfig = vi.fn((updater) => {
      state = typeof updater === 'function' ? updater(state) : updater;
    });

    render(
      <SdpRefreshControls
        jobConfig={state}
        setJobConfig={setJobConfig}
        datasets={sampleDatasets}
      />
    );

    // Non-persisted VIEW is excluded
    expect(
      screen.queryByTestId('sdp-refresh-checkbox-parsed_events_view')
    ).not.toBeInTheDocument();

    // Persisted datasets are present
    expect(screen.getByTestId('sdp-refresh-checkbox-bronze_orders')).toBeInTheDocument();
    expect(screen.getByTestId('sdp-refresh-checkbox-silver_customers')).toBeInTheDocument();
    expect(screen.getByTestId('sdp-refresh-checkbox-gold_store_revenue')).toBeInTheDocument();
    expect(screen.getByTestId('sdp-refresh-checkbox-kafka_orders_sink')).toBeInTheDocument();

    // Selecting silver_customers in --refresh removes it from sdpFullRefreshDatasets to prevent overlap
    fireEvent.click(screen.getByTestId('sdp-refresh-checkbox-silver_customers'));
    expect(typeof setJobConfig.mock.calls[0][0]).toBe('function');
    expect(state.sdpRefreshDatasets).toEqual(['silver_customers']);
    expect(state.sdpFullRefreshDatasets).toEqual([]);
  });

  it('disables non-resettable datasets (TABLE_NOT_RESETTABLE) in full_refresh mode and warns on external sinks', () => {
    let state: Record<string, unknown> = {
      sdpCommand: 'run',
      sdpRefreshMode: 'full_refresh',
      sdpFullRefreshDatasets: [],
    };
    const setJobConfig = vi.fn((updater) => {
      state = typeof updater === 'function' ? updater(state) : updater;
    });

    const { rerender } = render(
      <SdpRefreshControls
        jobConfig={state}
        setJobConfig={setJobConfig}
        datasets={sampleDatasets}
      />
    );

    const protectedCheckbox = screen.getByTestId(
      'sdp-full-refresh-checkbox-bronze_orders'
    ) as HTMLInputElement;
    expect(protectedCheckbox).toBeDisabled();
    expect(screen.getByTestId('sdp-not-resettable-tag-bronze_orders')).toHaveTextContent(
      'TABLE_NOT_RESETTABLE'
    );

    // Select external sink in full_refresh -> shows duplicate warning naming kafka_orders_sink
    fireEvent.click(screen.getByTestId('sdp-full-refresh-checkbox-kafka_orders_sink'));
    rerender(
      <SdpRefreshControls
        jobConfig={state}
        setJobConfig={setJobConfig}
        datasets={sampleDatasets}
      />
    );

    const sinkWarning = screen.getByTestId('sdp-external-sink-duplicate-warning');
    expect(sinkWarning).toHaveTextContent('kafka_orders_sink');
    expect(sinkWarning).toHaveTextContent('pipelines.reset.allowed');
    expect(sinkWarning).toHaveTextContent('duplicates');
  });

  it('displays external sink duplicate warning on full_refresh_all and MV truncation note whenever MVs exist', () => {
    render(
      <SdpRefreshControls
        jobConfig={{
          sdpCommand: 'run',
          sdpRefreshMode: 'full_refresh_all',
        }}
        setJobConfig={vi.fn()}
        datasets={sampleDatasets}
      />
    );

    const sinkWarning = screen.getByTestId('sdp-external-sink-duplicate-warning');
    expect(sinkWarning).toHaveTextContent('kafka_orders_sink');

    const mvNote = screen.getByTestId('sdp-mv-truncation-note');
    expect(mvNote).toHaveTextContent('gold_store_revenue');
    expect(mvNote).toHaveTextContent('TRUNCATE TABLE');
    expect(mvNote).toHaveTextContent('empty materialized views during a run and after a failed run');
  });

  it('switches to dry-run mode and hides refresh strategy selector', () => {
    let state: Record<string, unknown> = { sdpCommand: 'run', sdpRefreshMode: 'incremental' };
    const setJobConfig = vi.fn((updater) => {
      state = typeof updater === 'function' ? updater(state) : updater;
    });

    const { rerender } = render(
      <SdpRefreshControls
        jobConfig={state}
        setJobConfig={setJobConfig}
        datasets={sampleDatasets}
      />
    );

    fireEvent.change(screen.getByLabelText(/SDP Command/i, { selector: 'select' }), {
      target: { value: 'dry-run' },
    });
    expect(state.sdpCommand).toBe('dry-run');

    rerender(
      <SdpRefreshControls
        jobConfig={state}
        setJobConfig={setJobConfig}
        datasets={sampleDatasets}
      />
    );
    expect(screen.getByTestId('sdp-dry-run-note')).toBeInTheDocument();
    expect(screen.queryByLabelText(/Refresh Strategy/i)).not.toBeInTheDocument();
  });
});
