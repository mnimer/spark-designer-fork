/**
 * @fileoverview Unit tests for ForeachBatchSinkProperties.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x `ForeachBatchSink` form rendering across all 4 preset modes
 * (`custom_python`, `http_webhook`, `datadog`, `splunk_hec`), `endpointUrl` scheme
 * validation, `SecretManagerSecretField` `tokenSecret` validation, `httpMethod`,
 * `batchSize`, `timeoutSeconds`, `maxRetries`, `customFunctionInstructions`, and
 * `system_instructions`.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ForeachBatchSinkProperties } from './ForeachBatchSink';

describe('ForeachBatchSinkProperties', () => {
  let originalVscode: unknown;
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    originalVscode = (window as unknown as Record<string, unknown>).vscode;
    (window as unknown as Record<string, unknown>).vscode = { postMessage: vi.fn() };
  });

  afterEach(() => {
    (window as unknown as Record<string, unknown>).vscode = originalVscode;
  });

  it('renders custom_python mode by default and validates required Custom Python Function Logic', () => {
    render(
      <ForeachBatchSinkProperties
        node={{ data: { label: 'ForeachBatch Sink', spark_transform: 'ForeachBatchSink', properties: {} } }}
        onChange={mockOnChange}
      />
    );

    const modeSelect = screen.getByLabelText(/Sink Mode/i, { selector: 'select' }) as HTMLSelectElement;
    expect(Array.from(modeSelect.options).map((o) => o.value)).toEqual([
      'custom_python',
      'http_webhook',
      'datadog',
      'splunk_hec',
    ]);
    expect(modeSelect).toHaveValue('custom_python');
    expect(screen.getByText('Custom Python logic is required.')).toBeInTheDocument();
    expect(screen.queryByLabelText(/Endpoint URL/i, { selector: 'input' })).not.toBeInTheDocument();
  });

  it('renders HTTP controls and validates endpointUrl and required tokenSecret for datadog and splunk_hec', () => {
    const { rerender } = render(
      <ForeachBatchSinkProperties
        node={{
          data: {
            properties: {
              sinkMode: 'datadog',
              endpointUrl: 'ftp://invalid-scheme',
              tokenSecret: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText('Endpoint URL must start with https:// or http://.')).toBeInTheDocument();
    expect(screen.getByText('Secret Manager credential reference is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Datadog API Key Secret/i, { selector: 'input' })).toBeInTheDocument();

    rerender(
      <ForeachBatchSinkProperties
        node={{
          data: {
            properties: {
              sinkMode: 'splunk_hec',
              endpointUrl: 'https://splunk.example.com:8088/services/collector/event',
              tokenSecret: 'splunk-prod-hec',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.queryByText('Secret Manager credential reference is required.')
    ).not.toBeInTheDocument();
    expect(screen.getByLabelText(/Splunk HEC Token Secret/i, { selector: 'input' })).toHaveValue(
      'splunk-prod-hec'
    );
  });

  it('dispatches updates for http_webhook fields, SecretManagerSecretField, custom instructions, and system_instructions', () => {
    render(
      <ForeachBatchSinkProperties
        node={{
          data: {
            properties: {
              sinkMode: 'http_webhook',
              endpointUrl: 'https://api.example.com/v1/events',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/HTTP Method/i, { selector: 'select' }), {
      target: { name: 'httpMethod', value: 'PUT' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ httpMethod: 'PUT' }));

    fireEvent.change(screen.getByLabelText(/Batch Size/i, { selector: 'input' }), {
      target: { name: 'batchSize', value: '250' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ batchSize: '250' }));

    fireEvent.change(screen.getByLabelText(/Timeout \(Seconds\)/i, { selector: 'input' }), {
      target: { name: 'timeoutSeconds', value: '45' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ timeoutSeconds: '45' }));

    fireEvent.change(screen.getByLabelText(/Retry Count/i, { selector: 'input' }), {
      target: { name: 'maxRetries', value: '5' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ maxRetries: '5' }));

    fireEvent.change(screen.getByLabelText(/Bearer Token \/ API Key Secret/i, { selector: 'input' }), {
      target: { name: 'tokenSecret', value: 'webhook-token-secret' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ tokenSecret: 'webhook-token-secret' })
    );

    fireEvent.change(screen.getByLabelText(/Custom Payload Mapping/i, { selector: 'textarea' }), {
      target: { name: 'customFunctionInstructions', value: 'Wrap records in events array' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ customFunctionInstructions: 'Wrap records in events array' })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }), {
      target: { value: 'Add X-Tenant-Id header from tenant_id column' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Add X-Tenant-Id header from tenant_id column',
      })
    );
  });
});
