/**
 * @fileoverview Unit tests for the shared StorageBackendFields component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies storage backend selection, automatic scheme inference from URIs,
 * legacy filePattern and uri fallback resolution, GCS bucket autocomplete integration,
 * pathGlobFilter and recursiveFileLookup controls, and Secret Manager authentication
 * rendering for S3 and Azure storage backends.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  StorageBackendFields,
  inferStorageBackend,
  resolveStoragePathValue,
} from './StorageBackendFields';

describe('StorageBackendFields', () => {
  let originalVscode: typeof window.vscode;
  const mockOnChange = vi.fn();
  const mockPostMessage = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    originalVscode = window.vscode;
    window.vscode = { postMessage: mockPostMessage };
  });

  afterEach(() => {
    window.vscode = originalVscode;
  });

  describe('inferStorageBackend and resolveStoragePathValue helpers', () => {
    it('infers storageBackend from URI schemes accurately', () => {
      expect(inferStorageBackend('')).toBe('gcs');
      expect(inferStorageBackend('gs://my-bucket/data')).toBe('gcs');
      expect(inferStorageBackend('s3a://my-bucket/data')).toBe('s3a');
      expect(inferStorageBackend('s3://my-bucket/data')).toBe('s3a');
      expect(inferStorageBackend('abfss://container@acct.dfs.core.windows.net/path')).toBe('abfss');
      expect(inferStorageBackend('wasbs://container@acct.blob.core.windows.net/path')).toBe('wasbs');
      expect(inferStorageBackend('hdfs://namenode:8020/warehouse')).toBe('hdfs');
      expect(inferStorageBackend('file:///tmp/data')).toBe('local');
      expect(inferStorageBackend('/var/data/input.parquet')).toBe('local');
    });

    it('resolves path from pathKey with fallback to filePattern and uri', () => {
      expect(resolveStoragePathValue({ path: 'gs://b/p' })).toBe('gs://b/p');
      expect(resolveStoragePathValue({ filePattern: 's3a://b/f' })).toBe('s3a://b/f');
      expect(resolveStoragePathValue({ uri: 'hdfs://nn/u' })).toBe('hdfs://nn/u');
      expect(resolveStoragePathValue({ customPath: '/tmp/a' }, 'customPath')).toBe('/tmp/a');
      expect(resolveStoragePathValue({})).toBe('');
    });
  });

  it('renders GCS backend by default, requests storage buckets, and flags empty path as invalid', () => {
    const { container } = render(
      <StorageBackendFields
        properties={{}}
        onChange={mockOnChange}
        idPrefix="testStorage"
      />
    );

    const backendSelect = container.querySelector('#testStorageStorageBackend') as HTMLSelectElement;
    expect(backendSelect).toBeInTheDocument();
    expect(backendSelect).toHaveValue('gcs');

    const pathInput = container.querySelector('#testStoragePath') as HTMLInputElement;
    expect(pathInput).toBeInTheDocument();
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Storage URI / Path is required.')).toBeInTheDocument();

    expect(mockPostMessage).toHaveBeenCalledWith({
      command: 'getStorageBuckets',
    });

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'storageBucketsLoaded',
            buckets: ['spark-lakehouse-bucket'],
          },
        })
      );
    });

    const datalistId = pathInput.getAttribute('data-list') || pathInput.getAttribute('list');
    expect(datalistId).toBeTruthy();
    const datalist = document.getElementById(datalistId!);
    expect(datalist?.children[0]).toHaveAttribute('value', 'gs://spark-lakehouse-bucket/');
  });

  it('auto-infers s3a, abfss, wasbs, hdfs, and local backends from legacy filePattern or uri when storageBackend is unset', () => {
    const { container, rerender } = render(
      <StorageBackendFields
        properties={{ filePattern: 's3a://analytics-bucket/events/*.parquet' }}
        onChange={mockOnChange}
        idPrefix="testStorage"
      />
    );

    const backendSelect = container.querySelector('#testStorageStorageBackend') as HTMLSelectElement;
    expect(backendSelect).toHaveValue('s3a');
    expect(container.querySelector('#testStoragePath')).toHaveValue(
      's3a://analytics-bucket/events/*.parquet'
    );
    expect(container.querySelector('#testStorageAuthMode')).toBeInTheDocument();

    rerender(
      <StorageBackendFields
        properties={{ uri: 'abfss://lake@acct.dfs.core.windows.net/silver' }}
        onChange={mockOnChange}
        idPrefix="testStorage"
      />
    );
    expect(backendSelect).toHaveValue('abfss');
    expect(container.querySelector('#testStorageAuthMode')).toBeInTheDocument();

    rerender(
      <StorageBackendFields
        properties={{ path: 'hdfs://cluster-m:8020/user/spark/data' }}
        onChange={mockOnChange}
        idPrefix="testStorage"
      />
    );
    expect(backendSelect).toHaveValue('hdfs');
    expect(container.querySelector('#testStorageAuthMode')).not.toBeInTheDocument();
  });

  it('propagates changes for storageBackend, path, pathGlobFilter, and recursiveFileLookup', () => {
    const { container } = render(
      <StorageBackendFields
        properties={{ path: 'gs://my-bucket/input/' }}
        onChange={mockOnChange}
        idPrefix="testStorage"
      />
    );

    fireEvent.change(container.querySelector('#testStorageStorageBackend')!, {
      target: { name: 'storageBackend', value: 's3a' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ storageBackend: 's3a' })
    );

    fireEvent.change(container.querySelector('#testStoragePath')!, {
      target: { name: 'path', value: 'gs://my-bucket/updated/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ path: 'gs://my-bucket/updated/' })
    );

    fireEvent.change(container.querySelector('#testStoragePathGlobFilter')!, {
      target: { name: 'pathGlobFilter', value: '*.parquet' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ pathGlobFilter: '*.parquet' })
    );

    fireEvent.click(container.querySelector('#testStorageRecursiveFileLookup')!);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ recursiveFileLookup: true })
    );
  });

  it('hides pathGlobFilter and recursiveFileLookup when showGlobAndRecursive is false', () => {
    const { container } = render(
      <StorageBackendFields
        properties={{ path: 'gs://my-bucket/output/' }}
        onChange={mockOnChange}
        idPrefix="testStorage"
        showGlobAndRecursive={false}
      />
    );

    expect(container.querySelector('#testStoragePathGlobFilter')).not.toBeInTheDocument();
    expect(container.querySelector('#testStorageRecursiveFileLookup')).not.toBeInTheDocument();
  });

  it('renders SecretManagerAuthFields when storageBackend is s3a, abfss, or wasbs and updates secretName', () => {
    const { container } = render(
      <StorageBackendFields
        properties={{
          storageBackend: 'wasbs',
          path: 'wasbs://container@acct.blob.core.windows.net/data',
          authMode: 'secretManager',
          secretName: 'azure-blob-secret',
        }}
        onChange={mockOnChange}
        idPrefix="testStorage"
      />
    );

    const authSelect = container.querySelector('#testStorageAuthMode') as HTMLSelectElement;
    const secretInput = container.querySelector('#testStorageSecretName') as HTMLInputElement;
    expect(authSelect).toHaveValue('secretManager');
    expect(secretInput).toHaveValue('azure-blob-secret');

    fireEvent.change(secretInput, {
      target: { name: 'secretName', value: 'new-azure-secret' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ secretName: 'new-azure-secret' })
    );
  });
});
