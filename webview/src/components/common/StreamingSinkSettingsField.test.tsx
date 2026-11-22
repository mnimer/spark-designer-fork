/**
 * @fileoverview Unit tests for StreamingSinkSettingsField per-sink streaming overrides.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  StreamingSinkSettingsField,
  shouldShowStreamingSinkSettings,
} from './StreamingSinkSettingsField';

describe('StreamingSinkSettingsField', () => {
  it('returns false for non-sink or plain batch sink nodes and true when streaming or overrides exist', () => {
    expect(shouldShowStreamingSinkSettings({ data: { type: 'STEP', executionMode: 'streaming' } })).toBe(false);
    expect(shouldShowStreamingSinkSettings({ data: { type: 'SINK', executionMode: 'batch', properties: {} } })).toBe(false);
    expect(shouldShowStreamingSinkSettings({ data: { type: 'SINK', executionMode: 'streaming', properties: {} } })).toBe(true);
    expect(shouldShowStreamingSinkSettings({ data: { type: 'SINK', isStreaming: true, properties: {} } })).toBe(true);
    expect(
      shouldShowStreamingSinkSettings({
        data: { type: 'SINK', properties: { outputMode: 'complete' } },
      })
    ).toBe(true);
    expect(
      shouldShowStreamingSinkSettings({
        data: { type: 'SINK', properties: { checkpointLocation: 'gs://b/chk' } },
      })
    ).toBe(true);
  });

  it('does not render anything for batch sinks without streaming overrides', () => {
    const { container } = render(
      <StreamingSinkSettingsField
        node={{ id: 'sink-1', data: { type: 'SINK', executionMode: 'batch', properties: {} } }}
        onChange={vi.fn()}
      />
    );
    expect(container).toBeEmptyDOMElement();
  });

  it('renders defaults (append + inherit) and inheritance hint with nodeId on streaming sinks', () => {
    render(
      <StreamingSinkSettingsField
        node={{ id: 'iceberg_sink_1', data: { type: 'SINK', executionMode: 'streaming', properties: {} } }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByText('Streaming Sink Settings')).toBeInTheDocument();
    expect(screen.getByLabelText(/Output Mode/i)).toHaveValue('append');
    expect(screen.getByLabelText(/Trigger Mode/i)).toHaveValue('inherit');
    expect(screen.queryByLabelText(/Trigger Interval/i)).not.toBeInTheDocument();

    const checkpointInput = screen.getByLabelText(/Checkpoint Location Override/i);
    expect(checkpointInput).toHaveAttribute('placeholder', '<checkpointRoot>/iceberg_sink_1');
    expect(
      screen.getByText(/Leave blank to inherit <checkpointRoot>\/iceberg_sink_1/i)
    ).toBeInTheDocument();
  });

  it('updates outputMode, triggerType, contextual triggerInterval, and checkpointLocation', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <StreamingSinkSettingsField
        node={{
          id: 'delta_sink',
          data: {
            type: 'SINK',
            isStreaming: true,
            properties: { existingProp: 'keep-me' },
          },
        }}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Output Mode/i), { target: { value: 'update' } });
    expect(handleChange).toHaveBeenCalledWith({
      existingProp: 'keep-me',
      outputMode: 'update',
    });

    fireEvent.change(screen.getByLabelText(/Trigger Mode/i), {
      target: { value: 'processingTime' },
    });
    expect(handleChange).toHaveBeenCalledWith({
      existingProp: 'keep-me',
      triggerType: 'processingTime',
    });

    // Rerender with processingTime selected -> Trigger Interval input appears
    rerender(
      <StreamingSinkSettingsField
        node={{
          id: 'delta_sink',
          data: {
            type: 'SINK',
            isStreaming: true,
            properties: { triggerType: 'processingTime', triggerInterval: '5 seconds' },
          },
        }}
        onChange={handleChange}
      />
    );

    const intervalInput = screen.getByLabelText(/Trigger Interval/i);
    expect(intervalInput).toHaveValue('5 seconds');
    fireEvent.change(intervalInput, { target: { value: '15 seconds' } });
    expect(handleChange).toHaveBeenCalledWith({
      triggerType: 'processingTime',
      triggerInterval: '15 seconds',
    });

    // Rerender with availableNow -> Trigger Interval input hides
    rerender(
      <StreamingSinkSettingsField
        node={{
          id: 'delta_sink',
          data: {
            type: 'SINK',
            isStreaming: true,
            properties: { triggerType: 'availableNow' },
          },
        }}
        onChange={handleChange}
      />
    );
    expect(screen.queryByLabelText(/Trigger Interval/i)).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Checkpoint Location Override/i), {
      target: { value: 'gs://my-bucket/chk/delta_sink' },
    });
    expect(handleChange).toHaveBeenCalledWith({
      triggerType: 'availableNow',
      checkpointLocation: 'gs://my-bucket/chk/delta_sink',
    });
  });

  it('returns null in SDP mode to hide per-sink outputMode, trigger, and checkpointLocation (SDP2)', () => {
    expect(
      shouldShowStreamingSinkSettings(
        { data: { type: 'SINK', executionMode: 'streaming', properties: { outputMode: 'append' } } },
        'sdp'
      )
    ).toBe(false);

    const { container } = render(
      <StreamingSinkSettingsField
        node={{
          id: 'kafka-sdp',
          data: {
            type: 'SINK',
            executionMode: 'streaming',
            paradigm: 'sdp',
            properties: { outputMode: 'append', checkpointLocation: 'gs://b/chk' },
          },
        }}
        paradigm="sdp"
        onChange={vi.fn()}
      />
    );
    expect(container).toBeEmptyDOMElement();
  });
});
