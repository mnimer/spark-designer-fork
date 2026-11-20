/**
 * @fileoverview Unit tests for SdpSinkSettingsField component (SDP2).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SdpSinkSettingsField } from './SdpSinkSettingsField';

describe('SdpSinkSettingsField (SDP2)', () => {
  it('returns null when paradigm is imperative or when node is not a SINK', () => {
    const { container: impContainer } = render(
      <SdpSinkSettingsField
        node={{
          id: 'snk-1',
          data: { type: 'SINK', spark_transform: 'IcebergSink', properties: {} },
        }}
        paradigm="imperative"
        onChange={vi.fn()}
      />
    );
    expect(impContainer).toBeEmptyDOMElement();

    const { container: stepContainer } = render(
      <SdpSinkSettingsField
        node={{
          id: 'step-1',
          data: { type: 'STEP', spark_transform: 'Filter', paradigm: 'sdp', properties: {} },
        }}
        paradigm="sdp"
        onChange={vi.fn()}
      />
    );
    expect(stepContainer).toBeEmptyDOMElement();
  });

  it('defaults Lakehouse sinks to Catalog Table and streaming broker sinks to External Sink', () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <SdpSinkSettingsField
        node={{
          id: 'iceberg-1',
          data: {
            label: 'Silver Orders',
            type: 'SINK',
            spark_transform: 'IcebergSink',
            properties: {},
          },
        }}
        paradigm="sdp"
        onChange={onChange}
      />
    );

    expect(
      screen.getByLabelText(/SDP Target Mode/i, { selector: 'select' })
    ).toHaveValue('catalog_table');
    expect(
      screen.getByLabelText(/Catalog Table Name/i, { selector: 'input' })
    ).toHaveAttribute('placeholder', 'silver_orders');
    expect(
      screen.getByLabelText(/Catalog Table Format/i, { selector: 'select' })
    ).toHaveValue('iceberg');

    rerender(
      <SdpSinkSettingsField
        node={{
          id: 'kafka-1',
          data: {
            label: 'Order Events Out',
            type: 'SINK',
            spark_transform: 'KafkaSink',
            properties: {},
          },
        }}
        paradigm="sdp"
        onChange={onChange}
      />
    );

    expect(
      screen.getByLabelText(/SDP Target Mode/i, { selector: 'select' })
    ).toHaveValue('external_sink');
    expect(
      screen.getByLabelText(/External Sink Name/i, { selector: 'input' })
    ).toHaveAttribute('placeholder', 'order_events_out');
    expect(
      screen.getByLabelText(/External Sink Format/i, { selector: 'input' })
    ).toHaveValue('kafka');
    expect(screen.getByTestId('sdp-external-sink-reset-note')).toHaveTextContent(
      'dp.create_sink'
    );
  });

  it('enforces mutually exclusive partition_cols and cluster_by layout modes', () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <SdpSinkSettingsField
        node={{
          id: 'iceberg-layout',
          data: {
            label: 'Gold Sales',
            type: 'SINK',
            spark_transform: 'IcebergSink',
            properties: {
              sdpLayoutMode: 'partition_cols',
              sdpPartitionCols: 'event_date',
              sdpClusterBy: 'stale_cluster',
              clusterBy: 'legacy_cluster',
            },
          },
        }}
        paradigm="sdp"
        onChange={onChange}
      />
    );

    const layoutSelect = screen.getByLabelText(/Table Layout/i, { selector: 'select' });
    expect(layoutSelect).toHaveValue('partition_cols');

    const partitionInput = screen.getByLabelText(/Partition Columns/i, { selector: 'input' });
    expect(partitionInput).toHaveValue('event_date');

    // Switching layout to cluster_by clears sdpPartitionCols
    fireEvent.change(layoutSelect, { target: { value: 'cluster_by' } });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sdpLayoutMode: 'cluster_by',
        sdpPartitionCols: '',
      })
    );

    // Rerender in cluster_by mode and verify typing cluster_by clears partitionCols
    onChange.mockClear();
    rerender(
      <SdpSinkSettingsField
        node={{
          id: 'iceberg-layout',
          data: {
            label: 'Gold Sales',
            type: 'SINK',
            spark_transform: 'IcebergSink',
            properties: {
              sdpLayoutMode: 'cluster_by',
              sdpClusterBy: 'store_id',
              sdpPartitionCols: 'stale_part',
              partitionCols: 'legacy_part',
            },
          },
        }}
        paradigm="sdp"
        onChange={onChange}
      />
    );

    const clusterInput = screen.getByLabelText(/Cluster By Columns/i, { selector: 'input' });
    expect(clusterInput).toHaveValue('store_id');
    fireEvent.change(clusterInput, { target: { value: 'store_id, day' } });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sdpLayoutMode: 'cluster_by',
        sdpClusterBy: 'store_id, day',
        sdpPartitionCols: '',
        partitionCols: '',
      })
    );
  });

  it('toggles pipelines.reset.allowed and displays INVALID_RESETTABLE_DEPENDENCY guidance when unchecked', () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <SdpSinkSettingsField
        node={{
          id: 'iceberg-reset',
          data: {
            label: 'Bronze Orders',
            type: 'SINK',
            spark_transform: 'IcebergSink',
            properties: {},
          },
        }}
        paradigm="sdp"
        onChange={onChange}
      />
    );

    const resetCheckbox = screen.getByLabelText(/Allow Full Refresh Reset/i, {
      selector: 'input',
    });
    expect(resetCheckbox).toBeChecked();
    expect(screen.queryByTestId('sdp-reset-dependency-note')).not.toBeInTheDocument();

    fireEvent.click(resetCheckbox);
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sdpResetAllowed: false,
      })
    );

    rerender(
      <SdpSinkSettingsField
        node={{
          id: 'iceberg-reset',
          data: {
            label: 'Bronze Orders',
            type: 'SINK',
            spark_transform: 'IcebergSink',
            properties: { sdpResetAllowed: false },
          },
        }}
        paradigm="sdp"
        onChange={onChange}
      />
    );

    expect(screen.getByTestId('sdp-reset-dependency-note')).toHaveTextContent(
      'INVALID_RESETTABLE_DEPENDENCY'
    );
  });
});
