/**
 * @fileoverview Unit tests for the Spark Watermark step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  WatermarkProperties,
  WATERMARK_DELAY_PRESETS,
  buildWatermarkSystemInstruction,
} from './Watermark';

describe('WatermarkProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-watermark-1',
    data: {
      label: 'Watermark',
      spark_transform: 'Watermark',
      properties,
    },
  });

  it('renders placement guidance banner, eventTimeColumn input, delayThreshold default, and presets', () => {
    render(<WatermarkProperties node={createNode()} onChange={vi.fn()} />);

    expect(screen.getByTestId('watermark-placement-banner')).toBeInTheDocument();
    expect(screen.getByText(/Place Watermark after payload parsing/i)).toBeInTheDocument();

    const eventColInput = screen.getByLabelText(/Event Time Column/i);
    expect(eventColInput).toHaveValue('');
    expect(eventColInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Event time column is required.')).toBeInTheDocument();

    const delayInput = screen.getByLabelText(/Delay Threshold/i);
    expect(delayInput).toHaveValue('10 minutes');
    expect(delayInput).toHaveAttribute('aria-invalid', 'false');

    for (const preset of WATERMARK_DELAY_PRESETS) {
      expect(screen.getByRole('button', { name: preset })).toBeInTheDocument();
    }
  });

  it('emits updated eventTimeColumn and generated system_instructions on input change', () => {
    const handleChange = vi.fn();
    render(
      <WatermarkProperties
        node={createNode({ delayThreshold: '5 minutes' })}
        onChange={handleChange}
      />
    );

    const eventColInput = screen.getByLabelText(/Event Time Column/i);
    fireEvent.change(eventColInput, { target: { value: 'event_timestamp' } });

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        eventTimeColumn: 'event_timestamp',
        delayThreshold: '5 minutes',
        system_instructions: buildWatermarkSystemInstruction('event_timestamp', '5 minutes'),
      })
    );
  });

  it('supports both clicking preset chips and typing a custom delayThreshold interval', () => {
    const handleChange = vi.fn();
    render(
      <WatermarkProperties
        node={createNode({ eventTimeColumn: 'event_ts', delayThreshold: '10 minutes' })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: '5 seconds' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        eventTimeColumn: 'event_ts',
        delayThreshold: '5 seconds',
      })
    );

    const delayInput = screen.getByLabelText(/Delay Threshold/i);
    fireEvent.change(delayInput, { target: { value: '45 seconds' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        eventTimeColumn: 'event_ts',
        delayThreshold: '45 seconds',
      })
    );
  });

  it('marks delayThreshold invalid when cleared to whitespace', () => {
    render(
      <WatermarkProperties
        node={createNode({ eventTimeColumn: 'event_ts', delayThreshold: '   ' })}
        onChange={vi.fn()}
      />
    );

    const delayInput = screen.getByLabelText(/Delay Threshold/i);
    expect(delayInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Delay threshold is required.')).toBeInTheDocument();
  });

  it('updates system_instructions when Code Generation Instructions textarea changes', () => {
    const handleChange = vi.fn();
    render(
      <WatermarkProperties
        node={createNode({ eventTimeColumn: 'event_ts', delayThreshold: '1 hour' })}
        onChange={handleChange}
      />
    );

    const instructionsInput = screen.getByLabelText(/Code Generation Instructions/i);
    fireEvent.change(instructionsInput, { target: { value: 'Cast ISO-8601 string before watermark.' } });

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Cast ISO-8601 string before watermark.',
      })
    );
  });

  it('supports pre-watermark timestamp casting representations and custom datetime patterns', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <WatermarkProperties
        node={createNode({ eventTimeColumn: 'event_ms', delayThreshold: '2 minutes' })}
        onChange={handleChange}
      />
    );

    const sourceTypeSelect = screen.getByLabelText(/Input Timestamp Representation/i, {
      selector: 'select',
    });
    expect(sourceTypeSelect).toHaveValue('timestamp');

    fireEvent.change(sourceTypeSelect, { target: { value: 'epoch_millis' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        eventTimeColumn: 'event_ms',
        delayThreshold: '2 minutes',
        sourceTimestampType: 'epoch_millis',
        system_instructions: expect.stringContaining('F.timestamp_millis'),
      })
    );

    fireEvent.change(sourceTypeSelect, { target: { value: 'custom_format' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceTimestampType: 'custom_format',
        timestampFormat: 'yyyy-MM-dd HH:mm:ss',
      })
    );

    rerender(
      <WatermarkProperties
        node={createNode({
          eventTimeColumn: 'raw_ts',
          delayThreshold: '5 minutes',
          sourceTimestampType: 'custom_format',
          timestampFormat: '   ',
        })}
        onChange={handleChange}
      />
    );

    const formatInput = screen.getByLabelText(/Datetime Pattern/i, { selector: 'input' });
    expect(formatInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Datetime pattern is required for custom format.')).toBeInTheDocument();

    fireEvent.change(formatInput, { target: { value: 'dd/MM/yyyy HH:mm:ss' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceTimestampType: 'custom_format',
        timestampFormat: 'dd/MM/yyyy HH:mm:ss',
        system_instructions: buildWatermarkSystemInstruction(
          'raw_ts',
          '5 minutes',
          'custom_format',
          'dd/MM/yyyy HH:mm:ss'
        ),
      })
    );
  });
});
