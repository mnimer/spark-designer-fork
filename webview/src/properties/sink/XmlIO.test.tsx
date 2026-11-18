/**
 * @fileoverview Unit tests for the XmlSink / XmlIO sink properties panel (SNK17).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates default rendering, required URI and tag validation, StorageBackendFields
 * multi-cloud and Secret Manager integration, saveMode, compression codecs, partitionBy,
 * maxRecordsPerFile, rootTag, rowTag, declaration, nullValue, attributePrefix, valueTag,
 * and SystemInstructionsField updates.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { XmlIOSinkProperties, XmlSinkProperties } from './XmlIO';

describe('XmlIOSinkProperties (SNK17 XmlSink)', () => {
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

  it('exports XmlSinkProperties as an alias for XmlIOSinkProperties', () => {
    expect(XmlSinkProperties).toBe(XmlIOSinkProperties);
  });

  it('renders defaults, omits StreamingSinkSettingsField and NodeDocsCard, and flags empty path', () => {
    const mockNode: any = {
      data: {
        label: 'XmlSink',
        type: 'SINK',
        spark_transform: 'XmlSink',
        executionMode: 'streaming',
        properties: {},
      },
    };

    const { container } = render(
      <XmlIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    const pathInput = container.querySelector('#xmlSinkPath') as HTMLInputElement;
    expect(pathInput).toHaveValue('');
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Output XML URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#xmlSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#xmlSinkCompression')).toHaveValue('none');
    expect(container.querySelector('#xmlSinkRootTag')).toHaveValue('ROWS');
    expect(container.querySelector('#xmlSinkRowTag')).toHaveValue('ROW');
    expect(container.querySelector('#xmlSinkAttributePrefix')).toHaveValue('_');
    expect(container.querySelector('#xmlSinkValueTag')).toHaveValue('_VALUE');
    expect(screen.queryByText('Streaming Sink Settings')).not.toBeInTheDocument();
    expect(screen.queryByText('Docs ↗')).not.toBeInTheDocument();
  });

  it('flags validation errors when rootTag or rowTag are cleared to empty whitespace', () => {
    const mockNode: any = {
      data: {
        properties: {
          path: 'gs://my-bucket/xml/',
          rootTag: '   ',
          rowTag: '',
        },
      },
    };

    const { container } = render(
      <XmlIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#xmlSinkRootTag')).toHaveAttribute('aria-invalid', 'true');
    expect(container.querySelector('#xmlSinkRowTag')).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Root Tag is required.')).toBeInTheDocument();
    expect(screen.getByText('Row Tag is required.')).toBeInTheDocument();
  });

  it('renders populated S3A and XML structure properties and propagates all updates', () => {
    const mockNode: any = {
      data: {
        label: 'XmlSink',
        spark_transform: 'XmlSink',
        properties: {
          path: 's3a://partner-feeds/catalog_xml/',
          authMode: 'secretManager',
          secretName: 'aws-partner-feed-secret',
          saveMode: 'overwrite',
          compression: 'gzip',
          partitionBy: ['feed_date'],
          maxRecordsPerFile: 50000,
          rootTag: 'catalog',
          rowTag: 'book',
          declaration: 'version="1.0" encoding="UTF-8"',
          attributePrefix: '@',
          valueTag: '_text',
          nullValue: 'NIL',
          system_instructions: 'Coalesce to 1 XML file per partition',
        },
      },
    };

    const { container } = render(
      <XmlIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#xmlSinkStorageBackend')).toHaveValue('s3a');
    expect(container.querySelector('#xmlSinkPath')).toHaveValue('s3a://partner-feeds/catalog_xml/');
    expect(container.querySelector('#xmlSinkSaveMode')).toHaveValue('overwrite');
    expect(container.querySelector('#xmlSinkCompression')).toHaveValue('gzip');
    expect(container.querySelector('#xmlSinkPartitionBy')).toHaveValue('feed_date');
    expect(container.querySelector('#xmlSinkMaxRecordsPerFile')).toHaveValue(50000);
    expect(container.querySelector('#xmlSinkRootTag')).toHaveValue('catalog');
    expect(container.querySelector('#xmlSinkRowTag')).toHaveValue('book');
    expect(container.querySelector('#xmlSinkDeclaration')).toHaveValue(
      'version="1.0" encoding="UTF-8"'
    );
    expect(container.querySelector('#xmlSinkAttributePrefix')).toHaveValue('@');
    expect(container.querySelector('#xmlSinkValueTag')).toHaveValue('_text');
    expect(container.querySelector('#xmlSinkNullValue')).toHaveValue('NIL');
    expect(container.querySelector('#xmlSinkInstructions')).toHaveValue(
      'Coalesce to 1 XML file per partition'
    );

    fireEvent.change(container.querySelector('#xmlSinkRootTag')!, {
      target: { name: 'rootTag', value: 'orders' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ rootTag: 'orders' }));

    fireEvent.change(container.querySelector('#xmlSinkRowTag')!, {
      target: { name: 'rowTag', value: 'order' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ rowTag: 'order' }));

    fireEvent.change(container.querySelector('#xmlSinkDeclaration')!, {
      target: { name: 'declaration', value: 'version="1.0"' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ declaration: 'version="1.0"' })
    );

    fireEvent.change(container.querySelector('#xmlSinkAttributePrefix')!, {
      target: { name: 'attributePrefix', value: '_' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ attributePrefix: '_' }));

    fireEvent.change(container.querySelector('#xmlSinkValueTag')!, {
      target: { name: 'valueTag', value: '_VALUE' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ valueTag: '_VALUE' }));

    fireEvent.change(container.querySelector('#xmlSinkNullValue')!, {
      target: { name: 'nullValue', value: '' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ nullValue: '' }));

    fireEvent.change(container.querySelector('#xmlSinkInstructions')!, {
      target: { value: 'Use built-in Spark 4.0 xml format' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Use built-in Spark 4.0 xml format' })
    );
  });
});
