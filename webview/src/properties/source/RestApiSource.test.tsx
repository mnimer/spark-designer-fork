/**
 * @fileoverview Unit Tests for RestApiSourceProperties Component (EXT1).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates RestApiSourceProperties rendering, required field and HTTP/HTTPS URL checks,
 * pagination mode switching (none, offset_limit, cursor, link_header), parallel range
 * partitioning inputs and bounds validation, HTTP auth schemes with Secret Manager
 * integration, collapsible request settings, system instructions placement, and
 * credential redaction classification.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { RestApiSourceProperties } from './RestApiSource';
import { classifyKey } from '../../../../src/credentialRedaction';

describe('RestApiSourceProperties (EXT1)', () => {
  let postMessageSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.clearAllMocks();
    postMessageSpy = vi.fn();
    (window as unknown as { vscode?: { postMessage: typeof postMessageSpy } }).vscode = {
      postMessage: postMessageSpy,
    };
  });

  afterEach(() => {
    delete (window as unknown as { vscode?: unknown }).vscode;
  });

  it('renders default values and flags required Endpoint URL and Output Schema when empty', () => {
    const { container } = render(
      <RestApiSourceProperties node={{ data: { properties: {} } }} onChange={vi.fn()} />
    );

    const urlInput = screen.getByLabelText(/Endpoint URL/i, { selector: 'input' });
    const methodSelect = screen.getByLabelText(/HTTP Method/i, { selector: 'select' });
    const schemaTextarea = screen.getByLabelText(/Output Schema \(DDL\)/i, {
      selector: 'textarea',
    });
    const paginationSelect = screen.getByLabelText(/Pagination Mode/i, { selector: 'select' });
    const partitionsInput = screen.getByLabelText(/Number of Partitions/i, { selector: 'input' });
    const authSchemeSelect = screen.getByLabelText(/HTTP Auth Scheme/i, { selector: 'select' });

    expect(urlInput).toHaveValue('');
    expect(urlInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Endpoint URL is required.')).toBeInTheDocument();

    expect(schemaTextarea).toHaveValue('');
    expect(schemaTextarea).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Output Schema (DDL) is required.')).toBeInTheDocument();

    expect(methodSelect).toHaveValue('GET');
    expect(paginationSelect).toHaveValue('none');
    expect(partitionsInput).toHaveValue(1);
    expect(authSchemeSelect).toHaveValue('none');

    // Verify SystemInstructionsField sits as a direct child of the flex-1 flex-col root
    const root = container.firstElementChild;
    expect(root?.className).toContain('flex-1');
    expect(root?.className).toContain('flex-col');
    const instructionsWrapper = container.querySelector('[data-codegen-instructions="true"]');
    expect(instructionsWrapper?.parentElement).toBe(root);
  });

  it('validates malformed and non-HTTP URLs and accepts valid http and https endpoints', () => {
    const { rerender } = render(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              endpointUrl: 'ftp://files.example.com/orders.json',
              schemaDDL: 'id STRING, amount DOUBLE',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(
      screen.getByText('Endpoint URL must start with http:// or https:// and include a hostname.')
    ).toBeInTheDocument();

    rerender(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              endpointUrl: 'not-a-valid-url',
              schemaDDL: 'id STRING, amount DOUBLE',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(
      screen.getByText('Endpoint URL must start with http:// or https:// and include a hostname.')
    ).toBeInTheDocument();

    rerender(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              endpointUrl: 'http://localhost:8080/v1/events',
              schemaDDL: 'id STRING, amount DOUBLE',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.queryAllByRole('alert')).toHaveLength(0);

    rerender(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              endpointUrl: 'https://api.example.com/v1/orders?active=true',
              httpMethod: 'POST',
              schemaDDL: 'id STRING, status STRING, amount DOUBLE',
              recordsJsonPath: 'data.items',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.queryAllByRole('alert')).toHaveLength(0);
    expect(screen.getByLabelText(/HTTP Method/i, { selector: 'select' })).toHaveValue('POST');
    expect(screen.getByLabelText(/Records JSON Path/i, { selector: 'input' })).toHaveValue(
      'data.items'
    );
  });

  it('switches conditional pagination fields across none, offset_limit, cursor, and link_header modes', () => {
    const onChange = vi.fn();
    const baseProps = {
      endpointUrl: 'https://api.example.com/v1/orders',
      schemaDDL: 'id STRING',
    };

    const { rerender } = render(
      <RestApiSourceProperties
        node={{ data: { properties: { ...baseProps, paginationType: 'none' } } }}
        onChange={onChange}
      />
    );

    expect(screen.queryByLabelText(/Page Size/i, { selector: 'input' })).not.toBeInTheDocument();
    expect(
      screen.queryByLabelText(/Offset Parameter/i, { selector: 'input' })
    ).not.toBeInTheDocument();
    expect(
      screen.queryByLabelText(/Cursor Parameter/i, { selector: 'input' })
    ).not.toBeInTheDocument();

    // Switch to offset_limit
    rerender(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              ...baseProps,
              paginationType: 'offset_limit',
              pageSize: 250,
              maxPages: 50,
              offsetParam: 'start',
              limitParam: 'count',
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(screen.getByLabelText(/Page Size/i, { selector: 'input' })).toHaveValue(250);
    expect(screen.getByLabelText(/Max Pages/i, { selector: 'input' })).toHaveValue(50);
    const offsetInput = screen.getByLabelText(/Offset Parameter/i, { selector: 'input' });
    const limitInput = screen.getByLabelText(/Limit Parameter/i, { selector: 'input' });
    expect(offsetInput).toHaveValue('start');
    expect(limitInput).toHaveValue('count');

    fireEvent.change(offsetInput, { target: { name: 'offsetParam', value: 'skip' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ offsetParam: 'skip' }));

    // Switch to cursor
    rerender(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              ...baseProps,
              paginationType: 'cursor',
              cursorParam: 'page_token',
              nextCursorJsonPath: 'meta.next_token',
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(
      screen.queryByLabelText(/Offset Parameter/i, { selector: 'input' })
    ).not.toBeInTheDocument();
    const cursorInput = screen.getByLabelText(/Cursor Parameter/i, { selector: 'input' });
    const nextCursorPathInput = screen.getByLabelText(/Next Cursor JSON Path/i, {
      selector: 'input',
    });
    expect(cursorInput).toHaveValue('page_token');
    expect(nextCursorPathInput).toHaveValue('meta.next_token');

    fireEvent.change(nextCursorPathInput, {
      target: { name: 'nextCursorJsonPath', value: 'paging.after' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ nextCursorJsonPath: 'paging.after' })
    );

    // Switch to link_header
    rerender(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              ...baseProps,
              paginationType: 'link_header',
              pageSize: 100,
              maxPages: 25,
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(screen.getByLabelText(/Page Size/i, { selector: 'input' })).toHaveValue(100);
    expect(screen.getByLabelText(/Max Pages/i, { selector: 'input' })).toHaveValue(25);
    expect(
      screen.queryByLabelText(/Cursor Parameter/i, { selector: 'input' })
    ).not.toBeInTheDocument();
  });

  it('validates parallel partition inputs and boundary conditions on numeric bounds and counts', () => {
    const onChange = vi.fn();
    const validBase = {
      endpointUrl: 'https://api.example.com/v1/events',
      schemaDDL: 'id BIGINT, payload STRING',
    };

    const { rerender } = render(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              ...validBase,
              paginationType: 'offset_limit',
              pageSize: 0,
              maxPages: 0,
              numPartitions: 0,
              partitionParam: 'id',
              lowerBound: 500,
              upperBound: 100,
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(
      screen.getByText('Page Size must be an integer greater than or equal to 1.')
    ).toBeInTheDocument();
    expect(
      screen.getByText('Max Pages must be an integer greater than or equal to 1.')
    ).toBeInTheDocument();
    expect(
      screen.getByText('Number of Partitions must be an integer greater than or equal to 1.')
    ).toBeInTheDocument();
    expect(
      screen.getByText('Lower Bound must be less than or equal to Upper Bound.')
    ).toBeInTheDocument();

    // Equal lowerBound and upperBound is a valid single-point boundary
    rerender(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              ...validBase,
              numPartitions: 4,
              partitionParam: 'id',
              lowerBound: 100,
              upperBound: 100,
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(screen.queryAllByRole('alert')).toHaveLength(0);

    fireEvent.change(screen.getByLabelText(/Upper Bound/i, { selector: 'input' }), {
      target: { name: 'upperBound', value: '10000' },
    });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ upperBound: '10000' }));
  });

  it('configures HTTP auth schemes, custom header names, and Secret Manager credentials', () => {
    const onChange = vi.fn();
    const baseProps = {
      endpointUrl: 'https://api.example.com/v1/orders',
      schemaDDL: 'id STRING',
    };

    const { rerender } = render(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              ...baseProps,
              authScheme: 'api_key',
              authMode: 'secretManager',
              secretName: '',
            },
          },
        }}
        onChange={onChange}
      />
    );

    const headerInput = screen.getByLabelText(/Auth Header Name/i, { selector: 'input' });
    expect(headerInput).toHaveAttribute('placeholder', 'X-API-Key');
    expect(
      screen.getByText('Secret Manager Secret is required when Secret Manager is selected.')
    ).toBeInTheDocument();

    const secretInput = screen.getByLabelText(/Secret Manager Secret/i, { selector: 'input' });
    fireEvent.change(secretInput, {
      target: { name: 'secretName', value: 'projects/p/secrets/rest-key/versions/latest' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secretName: 'projects/p/secrets/rest-key/versions/latest',
      })
    );

    rerender(
      <RestApiSourceProperties
        node={{
          data: {
            properties: {
              ...baseProps,
              authScheme: 'bearer',
              authMode: 'secretManager',
              secretName: 'rest-token-secret',
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(screen.getByLabelText(/Auth Header Name/i, { selector: 'input' })).toHaveAttribute(
      'placeholder',
      'Authorization'
    );
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
  });

  it('toggles collapsible custom headers, request body, timeout, retries, and updates system instructions', () => {
    const onChange = vi.fn();
    render(
      <RestApiSourceProperties
        node={{
          data: {
            special_instructions: 'Legacy instruction fallback',
            properties: {
              endpointUrl: 'https://api.example.com/v1/search',
              httpMethod: 'POST',
              schemaDDL: 'id STRING',
            },
          },
        }}
        onChange={onChange}
      />
    );

    const toggleBtn = screen.getByRole('button', { name: /Custom Headers, Body & Retries/i });
    expect(toggleBtn).toHaveAttribute('aria-expanded', 'false');
    expect(
      screen.queryByLabelText(/Custom Headers \(JSON\)/i, { selector: 'textarea' })
    ).not.toBeInTheDocument();

    fireEvent.click(toggleBtn);
    expect(toggleBtn).toHaveAttribute('aria-expanded', 'true');

    const headersArea = screen.getByLabelText(/Custom Headers \(JSON\)/i, {
      selector: 'textarea',
    });
    const bodyArea = screen.getByLabelText(/Request Body \(POST\)/i, { selector: 'textarea' });
    const timeoutInput = screen.getByLabelText(/Timeout \(Seconds\)/i, { selector: 'input' });
    const retriesInput = screen.getByLabelText(/Max Retries/i, { selector: 'input' });

    fireEvent.change(headersArea, {
      target: { name: 'requestHeaders', value: '{"X-Tenant": "acme"}' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ requestHeaders: '{"X-Tenant": "acme"}' })
    );

    fireEvent.change(bodyArea, {
      target: { name: 'requestBody', value: '{"status": "OPEN"}' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ requestBody: '{"status": "OPEN"}' })
    );

    fireEvent.change(timeoutInput, { target: { name: 'timeoutSeconds', value: '60' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ timeoutSeconds: '60' }));

    fireEvent.change(retriesInput, { target: { name: 'maxRetries', value: '5' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ maxRetries: '5' }));

    const instructionsArea = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    expect(instructionsArea).toHaveValue('Legacy instruction fallback');
    fireEvent.change(instructionsArea, {
      target: { value: 'Deduplicate paginated records by order id' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Deduplicate paginated records by order id',
      })
    );
  });

  it('keeps non-secret RestApiSource property keys safe and Secret Manager pointers classified as reference', () => {
    const safeKeys = [
      'endpointUrl',
      'httpMethod',
      'schemaDDL',
      'recordsJsonPath',
      'paginationType',
      'pageSize',
      'maxPages',
      'offsetParam',
      'limitParam',
      'cursorParam',
      'nextCursorJsonPath',
      'numPartitions',
      'partitionParam',
      'lowerBound',
      'upperBound',
      'authScheme',
      'authHeaderName',
      'requestHeaders',
      'requestBody',
      'timeoutSeconds',
      'maxRetries',
    ];
    for (const key of safeKeys) {
      expect(classifyKey(key), key).toBe('safe');
    }
    expect(classifyKey('authMode')).toBe('reference');
    expect(classifyKey('secretName')).toBe('reference');
  });
});
