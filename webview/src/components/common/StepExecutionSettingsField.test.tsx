/**
 * @fileoverview Unit tests for StepExecutionSettingsField (Step Cache Output A11 and Source Read Mode).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  StepExecutionSettingsField,
  shouldShowSourceReadModeToggle,
  shouldShowStepCacheToggle,
  normalizeCacheOutputSelection,
} from './StepExecutionSettingsField';

describe('StepExecutionSettingsField', () => {
  describe('predicate and normalization helpers', () => {
    it('detects dual-mode sources via supportedReadModes or explicit readMode property', () => {
      expect(
        shouldShowSourceReadModeToggle({
          data: { type: 'SOURCE', supportedReadModes: ['batch', 'streaming'], properties: {} },
        })
      ).toBe(true);
      expect(
        shouldShowSourceReadModeToggle({
          data: { type: 'SOURCE', supportedReadModes: ['batch'], properties: {} },
        })
      ).toBe(false);
      expect(
        shouldShowSourceReadModeToggle({
          data: { type: 'SOURCE', properties: { readMode: 'streaming' } },
        })
      ).toBe(true);
      expect(
        shouldShowSourceReadModeToggle({
          data: { type: 'STEP', supportedReadModes: ['batch', 'streaming'] },
        })
      ).toBe(false);
    });

    it('detects batch steps and excludes streaming steps', () => {
      expect(shouldShowStepCacheToggle({ data: { type: 'STEP', executionMode: 'batch' } })).toBe(true);
      expect(shouldShowStepCacheToggle({ data: { type: 'STEP' } })).toBe(true);
      expect(shouldShowStepCacheToggle({ data: { type: 'STEP', executionMode: 'streaming' } })).toBe(false);
      expect(shouldShowStepCacheToggle({ data: { type: 'STEP', isStreaming: true } })).toBe(false);
    });

    it('normalizes cacheOutput values across boolean and string forms', () => {
      expect(normalizeCacheOutputSelection(undefined)).toBe('auto');
      expect(normalizeCacheOutputSelection('auto')).toBe('auto');
      expect(normalizeCacheOutputSelection(true)).toBe('true');
      expect(normalizeCacheOutputSelection('always')).toBe('true');
      expect(normalizeCacheOutputSelection(false)).toBe('false');
      expect(normalizeCacheOutputSelection('never')).toBe('false');
    });
  });

  it('renders Cache Output selector on batch STEP nodes and emits auto, true, and false', () => {
    const handleChange = vi.fn();
    render(
      <StepExecutionSettingsField
        node={{
          id: 'step-1',
          data: {
            type: 'STEP',
            executionMode: 'batch',
            properties: { property: 'status' },
          },
        }}
        onChange={handleChange}
      />
    );

    const cacheSelect = screen.getByLabelText(/Cache Output/i);
    expect(cacheSelect).toHaveValue('auto');

    fireEvent.change(cacheSelect, { target: { value: 'true' } });
    expect(handleChange).toHaveBeenCalledWith({
      property: 'status',
      cacheOutput: true,
    });

    fireEvent.change(cacheSelect, { target: { value: 'false' } });
    expect(handleChange).toHaveBeenCalledWith({
      property: 'status',
      cacheOutput: false,
    });

    fireEvent.change(cacheSelect, { target: { value: 'auto' } });
    expect(handleChange).toHaveBeenCalledWith({
      property: 'status',
      cacheOutput: 'auto',
    });
  });

  it('hides Cache Output selector on streaming STEP nodes', () => {
    const { container } = render(
      <StepExecutionSettingsField
        node={{
          id: 'step-stream-1',
          data: {
            type: 'STEP',
            executionMode: 'streaming',
            properties: {},
          },
        }}
        onChange={vi.fn()}
      />
    );
    expect(container).toBeEmptyDOMElement();
  });

  it('renders Read Mode selector on dual-mode SOURCE nodes and toggles between batch and streaming', () => {
    const handleChange = vi.fn();
    render(
      <StepExecutionSettingsField
        node={{
          id: 'src-kafka-1',
          data: {
            type: 'SOURCE',
            supportedReadModes: ['batch', 'streaming'],
            defaultReadMode: 'streaming',
            properties: { topics: 'events' },
          },
        }}
        onChange={handleChange}
      />
    );

    const readModeSelect = screen.getByLabelText(/Read Mode/i);
    expect(readModeSelect).toHaveValue('streaming');

    fireEvent.change(readModeSelect, { target: { value: 'batch' } });
    expect(handleChange).toHaveBeenCalledWith({
      topics: 'events',
      readMode: 'batch',
    });

    fireEvent.change(readModeSelect, { target: { value: 'streaming' } });
    expect(handleChange).toHaveBeenCalledWith({
      topics: 'events',
      readMode: 'streaming',
    });
  });

  it('hides manual Cache Output toggle in SDP mode and exposes SDP Dataset Persistence selector (SDP2)', () => {
    expect(
      shouldShowStepCacheToggle({ data: { type: 'STEP', executionMode: 'batch' } }, 'sdp')
    ).toBe(false);

    const handleChange = vi.fn();
    render(
      <StepExecutionSettingsField
        node={{
          id: 'step-sdp-1',
          data: {
            type: 'STEP',
            spark_transform: 'Filter',
            executionMode: 'batch',
            paradigm: 'sdp',
            properties: {},
          },
        }}
        paradigm="sdp"
        onChange={handleChange}
      />
    );

    expect(screen.queryByLabelText(/Cache Output/i)).not.toBeInTheDocument();
    const sdpPersistenceSelect = screen.getByLabelText(/SDP Dataset Persistence/i, {
      selector: 'select',
    });
    expect(sdpPersistenceSelect).toHaveValue('auto');

    fireEvent.change(sdpPersistenceSelect, { target: { value: 'temporary_view' } });
    expect(handleChange).toHaveBeenCalledWith({
      sdpDatasetType: 'temporary_view',
    });
  });
});
