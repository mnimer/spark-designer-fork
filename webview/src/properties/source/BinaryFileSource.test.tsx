/**
 * @fileoverview Unit tests for the BinaryFileSource properties panel (SRC20).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates rendering, readMode switching, StorageBackendFields integration
 * (including pathGlobFilter and recursiveFileLookup), maxFilesPerTrigger,
 * and SystemInstructionsField for BinaryFileSourceProperties.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { VsCodeApi } from '../../bridge/vscodeApi';
import {
  BinaryFileSourceProperties,
  BinaryFileIOSourceProperties,
  type BinaryFileSourceNode,
} from './BinaryFileSource';

describe('BinaryFileSourceProperties (SRC20 BinaryFileSource)', () => {
  const mockOnChange = vi.fn<(properties: Record<string, unknown>) => void>();
  let originalVscode: VsCodeApi | undefined;

  beforeEach(() => {
    vi.clearAllMocks();
    originalVscode = window.vscode;
    window.vscode = { postMessage: vi.fn() };
  });

  afterEach(() => {
    window.vscode = originalVscode;
  });

  it('exports BinaryFileIOSourceProperties as an alias for BinaryFileSourceProperties', () => {
    expect(BinaryFileIOSourceProperties).toBe(BinaryFileSourceProperties);
  });

  it('renders default batch settings and flags required validation when path is empty', () => {
    const mockNode: BinaryFileSourceNode = {
      data: {
        label: 'BinaryFileSource',
        spark_transform: 'BinaryFileSource',
        properties: {},
      },
    };

    const { container } = render(
      <BinaryFileSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#binaryFileSourceReadMode')).toHaveValue('batch');
    expect(container.querySelector('#binaryFileSourcePath')).toHaveAttribute(
      'aria-invalid',
      'true'
    );
    expect(screen.getByText('Binary File URI / Directory is required.')).toBeInTheDocument();
    expect(container.querySelector('#binaryFileSourcePathGlobFilter')).toBeInTheDocument();
    expect(container.querySelector('#binaryFileSourceRecursiveFileLookup')).toBeInTheDocument();
    expect(
      container.querySelector('#binaryFileSourceMaxFilesPerTrigger')
    ).not.toBeInTheDocument();
  });

  it('renders streaming settings and propagates changes for pathGlobFilter, recursiveFileLookup, maxFilesPerTrigger, and instructions', () => {
    const mockNode: BinaryFileSourceNode = {
      data: {
        label: 'BinaryFileSource',
        spark_transform: 'BinaryFileSource',
        properties: {
          readMode: 'streaming',
          path: 'gs://my-bucket/assets/',
          pathGlobFilter: '*.png',
          recursiveFileLookup: true,
          maxFilesPerTrigger: 25,
          system_instructions: 'Filter images over 10KB',
        },
      },
    };

    const { container } = render(
      <BinaryFileSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#binaryFileSourceReadMode')).toHaveValue('streaming');
    expect(container.querySelector('#binaryFileSourcePath')).toHaveValue('gs://my-bucket/assets/');
    expect(container.querySelector('#binaryFileSourcePathGlobFilter')).toHaveValue('*.png');
    expect(container.querySelector('#binaryFileSourceRecursiveFileLookup')).toBeChecked();
    expect(container.querySelector('#binaryFileSourceMaxFilesPerTrigger')).toHaveValue(25);

    fireEvent.change(container.querySelector('#binaryFileSourcePathGlobFilter')!, {
      target: { name: 'pathGlobFilter', value: '*.pdf' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ pathGlobFilter: '*.pdf' })
    );

    fireEvent.click(container.querySelector('#binaryFileSourceRecursiveFileLookup')!);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ recursiveFileLookup: false })
    );

    fireEvent.change(container.querySelector('#binaryFileSourceMaxFilesPerTrigger')!, {
      target: { name: 'maxFilesPerTrigger', value: '10' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxFilesPerTrigger: '10' })
    );

    fireEvent.change(container.querySelector('#binaryFileSourceInstructions')!, {
      target: { value: 'Extract PDF text' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Extract PDF text' })
    );
  });
});
