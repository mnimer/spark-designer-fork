/**
 * @fileoverview Unit tests for the TextSource / TextIO source properties panel (SRC18).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates rendering, readMode switching, StorageBackendFields integration,
 * wholetext toggle, lineSep input, maxFilesPerTrigger, and SystemInstructionsField
 * for TextIOSourceProperties.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { TextIOSourceProperties, TextSourceProperties } from './TextIO';

describe('TextIOSourceProperties (SRC18 TextSource)', () => {
  const mockOnChange = vi.fn();
  let originalVscode: any;

  beforeEach(() => {
    vi.clearAllMocks();
    originalVscode = (window as any).vscode;
    (window as any).vscode = { postMessage: vi.fn() };
  });

  afterEach(() => {
    (window as any).vscode = originalVscode;
  });

  it('exports TextSourceProperties as an alias for TextIOSourceProperties', () => {
    expect(TextSourceProperties).toBe(TextIOSourceProperties);
  });

  it('renders default batch settings and flags required validation when path is empty', () => {
    const mockNode: any = {
      data: {
        label: 'TextSource',
        spark_transform: 'TextSource',
        properties: {},
      },
    };

    const { container } = render(
      <TextIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#textSourceReadMode')).toHaveValue('batch');
    expect(container.querySelector('#textSourcePath')).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Text URI / Path is required.')).toBeInTheDocument();
    expect(container.querySelector('#textSourceWholetext')).not.toBeChecked();
    expect(container.querySelector('#textSourceLineSep')).toHaveValue('');
  });

  it('renders populated streaming properties and propagates updates for wholetext, lineSep, maxFilesPerTrigger, and instructions', () => {
    const mockNode: any = {
      data: {
        label: 'TextSource',
        spark_transform: 'TextSource',
        properties: {
          readMode: 'streaming',
          path: 'gs://my-bucket/logs/*.log',
          wholetext: false,
          lineSep: '\\n',
          maxFilesPerTrigger: 15,
          system_instructions: 'Parse log lines',
        },
      },
    };

    const { container } = render(
      <TextIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#textSourceReadMode')).toHaveValue('streaming');
    expect(container.querySelector('#textSourcePath')).toHaveValue('gs://my-bucket/logs/*.log');
    expect(container.querySelector('#textSourceLineSep')).toHaveValue('\\n');
    expect(container.querySelector('#textSourceMaxFilesPerTrigger')).toHaveValue(15);

    fireEvent.change(container.querySelector('#textSourceLineSep')!, {
      target: { name: 'lineSep', value: '\\r\\n' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ lineSep: '\\r\\n' }));

    fireEvent.click(container.querySelector('#textSourceWholetext')!);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ wholetext: true }));

    fireEvent.change(container.querySelector('#textSourceMaxFilesPerTrigger')!, {
      target: { name: 'maxFilesPerTrigger', value: '30' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxFilesPerTrigger: '30' })
    );

    fireEvent.change(container.querySelector('#textSourceSystemInstructions')!, {
      target: { value: 'Updated text instructions' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Updated text instructions' })
    );
  });
});
