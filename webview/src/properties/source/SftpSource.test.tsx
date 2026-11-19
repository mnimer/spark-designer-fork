/**
 * @fileoverview Unit Tests for SftpSourceProperties Component (EXT2).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates SftpSourceProperties rendering, required field checks, port range boundary
 * validation (1..65535), format-specific options across csv, json, parquet, text, and xml,
 * SSH key vs password auth with Secret Manager integration, collapsible host key and
 * partition sharding settings, system instructions placement, and credential redaction safety.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SftpSourceProperties } from './SftpSource';
import { classifyKey } from '../../../../src/credentialRedaction';

describe('SftpSourceProperties (EXT2)', () => {
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

  it('renders defaults (port 22, csv, ssh_key) and validates required empty fields', () => {
    const { container } = render(
      <SftpSourceProperties node={{ data: { properties: {} } }} onChange={vi.fn()} />
    );

    const hostInput = screen.getByLabelText(/SFTP Host/i, { selector: 'input' });
    const portInput = screen.getByLabelText(/Port/i, { selector: 'input' });
    const userInput = screen.getByLabelText(/Username/i, { selector: 'input' });
    const pathInput = screen.getByLabelText(/Remote Path/i, { selector: 'input' });
    const formatSelect = screen.getByLabelText(/File Format/i, { selector: 'select' });
    const schemaTextarea = screen.getByLabelText(/Output Schema \(DDL\)/i, {
      selector: 'textarea',
    });
    const authTypeSelect = screen.getByLabelText(/SSH Auth Type/i, { selector: 'select' });

    expect(hostInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Host is required.')).toBeInTheDocument();

    expect(portInput).toHaveValue(22);
    expect(portInput).toHaveAttribute('aria-invalid', 'false');

    expect(userInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Username is required.')).toBeInTheDocument();

    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Remote Path is required.')).toBeInTheDocument();

    expect(schemaTextarea).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Output Schema (DDL) is required.')).toBeInTheDocument();

    expect(formatSelect).toHaveValue('csv');
    expect(authTypeSelect).toHaveValue('ssh_key');

    // Verify SystemInstructionsField sits as a direct child of the flex-1 flex-col root
    const root = container.firstElementChild;
    expect(root?.className).toContain('flex-1');
    expect(root?.className).toContain('flex-col');
    const instructionsWrapper = container.querySelector('[data-codegen-instructions="true"]');
    expect(instructionsWrapper?.parentElement).toBe(root);
  });

  it('validates port boundaries (1..65535), host whitespace, and accepts valid configuration', () => {
    const validBase = {
      host: 'sftp.partner.example.com',
      port: 22,
      username: 'spark_ingest',
      remotePath: '/incoming/daily',
      filePattern: '*.csv',
      schemaDDL: 'id STRING, total STRING',
    };

    const { rerender } = render(
      <SftpSourceProperties
        node={{ data: { properties: { ...validBase, host: 'bad host', port: 0 } } }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByText('Host cannot contain whitespace.')).toBeInTheDocument();
    expect(
      screen.getByText('Port must be an integer between 1 and 65535.')
    ).toBeInTheDocument();

    // Upper out-of-range port 65536
    rerender(
      <SftpSourceProperties
        node={{ data: { properties: { ...validBase, port: 65536 } } }}
        onChange={vi.fn()}
      />
    );
    expect(
      screen.getByText('Port must be an integer between 1 and 65535.')
    ).toBeInTheDocument();

    // Empty port string
    rerender(
      <SftpSourceProperties
        node={{ data: { properties: { ...validBase, port: '' } } }}
        onChange={vi.fn()}
      />
    );
    expect(
      screen.getByText('Port must be an integer between 1 and 65535.')
    ).toBeInTheDocument();

    // Valid lower boundary port 1
    rerender(
      <SftpSourceProperties
        node={{ data: { properties: { ...validBase, port: 1 } } }}
        onChange={vi.fn()}
      />
    );
    expect(screen.queryAllByRole('alert')).toHaveLength(0);

    // Valid upper boundary port 65535
    rerender(
      <SftpSourceProperties
        node={{ data: { properties: { ...validBase, port: 65535 } } }}
        onChange={vi.fn()}
      />
    );
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
  });

  it('switches conditional format options across csv, json, xml, parquet, and text', () => {
    const onChange = vi.fn();
    const validBase = {
      host: 'sftp.example.com',
      port: 2222,
      username: 'etl',
      remotePath: '/exports',
      schemaDDL: 'col1 STRING',
    };

    const { rerender } = render(
      <SftpSourceProperties
        node={{
          data: {
            properties: {
              ...validBase,
              fileFormat: 'csv',
              csvHeader: true,
              csvDelimiter: '|',
            },
          },
        }}
        onChange={onChange}
      />
    );

    const headerCheck = screen.getByLabelText(/First Row Contains Header/i, {
      selector: 'input',
    });
    const delimiterInput = screen.getByLabelText(/CSV Delimiter/i, { selector: 'input' });
    expect(headerCheck).toBeChecked();
    expect(delimiterInput).toHaveValue('|');

    fireEvent.click(headerCheck);
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ csvHeader: false }));

    // Switch to json
    rerender(
      <SftpSourceProperties
        node={{
          data: {
            properties: {
              ...validBase,
              fileFormat: 'json',
              jsonMultiLine: false,
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(
      screen.queryByLabelText(/CSV Delimiter/i, { selector: 'input' })
    ).not.toBeInTheDocument();
    const multiLineCheck = screen.getByLabelText(/Multi-Line JSON Payload/i, {
      selector: 'input',
    });
    expect(multiLineCheck).not.toBeChecked();
    fireEvent.click(multiLineCheck);
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ jsonMultiLine: true }));

    // Switch to xml with empty xmlRowTag validation and populated xmlRowTag
    rerender(
      <SftpSourceProperties
        node={{
          data: {
            properties: {
              ...validBase,
              fileFormat: 'xml',
              xmlRowTag: '   ',
            },
          },
        }}
        onChange={onChange}
      />
    );

    expect(
      screen.getByText('XML Row Tag is required when File Format is XML.')
    ).toBeInTheDocument();

    const rowTagInput = screen.getByLabelText(/XML Row Tag/i, { selector: 'input' });
    fireEvent.change(rowTagInput, { target: { name: 'xmlRowTag', value: 'invoice' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ xmlRowTag: 'invoice' }));

    // Switch to parquet and text (no extra format-specific inputs)
    for (const fmt of ['parquet', 'text'] as const) {
      rerender(
        <SftpSourceProperties
          node={{ data: { properties: { ...validBase, fileFormat: fmt } } }}
          onChange={onChange}
        />
      );
      expect(
        screen.queryByLabelText(/CSV Delimiter/i, { selector: 'input' })
      ).not.toBeInTheDocument();
      expect(
        screen.queryByLabelText(/Multi-Line JSON Payload/i, { selector: 'input' })
      ).not.toBeInTheDocument();
      expect(
        screen.queryByLabelText(/XML Row Tag/i, { selector: 'input' })
      ).not.toBeInTheDocument();
      expect(screen.queryAllByRole('alert')).toHaveLength(0);
    }
  });

  it('configures SSH authType (ssh_key vs password) and Secret Manager credentials', () => {
    const onChange = vi.fn();
    const validBase = {
      host: 'sftp.example.com',
      port: 22,
      username: 'etl',
      remotePath: '/data',
      schemaDDL: 'id STRING',
    };

    render(
      <SftpSourceProperties
        node={{
          data: {
            properties: {
              ...validBase,
              authType: 'password',
              authMode: 'secretManager',
              secretName: '',
            },
          },
        }}
        onChange={onChange}
      />
    );

    const authTypeSelect = screen.getByLabelText(/SSH Auth Type/i, { selector: 'select' });
    expect(authTypeSelect).toHaveValue('password');
    expect(
      screen.getByText('Secret Manager Secret is required when Secret Manager is selected.')
    ).toBeInTheDocument();

    fireEvent.change(authTypeSelect, { target: { name: 'authType', value: 'ssh_key' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ authType: 'ssh_key' }));

    const secretInput = screen.getByLabelText(/Secret Manager Secret/i, { selector: 'input' });
    fireEvent.change(secretInput, {
      target: { name: 'secretName', value: 'projects/p/secrets/sftp-pem/versions/1' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secretName: 'projects/p/secrets/sftp-pem/versions/1',
      })
    );
  });

  it('toggles collapsible host key verification and sharding settings and updates system instructions', () => {
    const onChange = vi.fn();
    render(
      <SftpSourceProperties
        node={{
          data: {
            system_instructions: 'Initial SFTP guidance',
            properties: {
              host: 'sftp.example.com',
              port: 22,
              username: 'etl',
              remotePath: '/data',
              schemaDDL: 'id STRING',
              numPartitions: 0,
            },
          },
        }}
        onChange={onChange}
      />
    );

    const toggleBtn = screen.getByRole('button', {
      name: /Host Key Verification & Sharding/i,
    });
    expect(toggleBtn).toHaveAttribute('aria-expanded', 'false');

    fireEvent.click(toggleBtn);
    expect(toggleBtn).toHaveAttribute('aria-expanded', 'true');

    expect(
      screen.getByText('Number of Partitions must be an integer greater than or equal to 1.')
    ).toBeInTheDocument();

    const strictCheck = screen.getByLabelText(/Strict Host Key Checking/i, {
      selector: 'input',
    });
    expect(strictCheck).toBeChecked();
    fireEvent.click(strictCheck);
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ strictHostKeyChecking: false })
    );

    const knownHostsInput = screen.getByLabelText(/Known Hosts File Path/i, {
      selector: 'input',
    });
    fireEvent.change(knownHostsInput, {
      target: { name: 'knownHostsPath', value: '/etc/ssh/ssh_known_hosts' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ knownHostsPath: '/etc/ssh/ssh_known_hosts' })
    );

    const partitionsInput = screen.getByLabelText(/Number of Partitions/i, {
      selector: 'input',
    });
    fireEvent.change(partitionsInput, { target: { name: 'numPartitions', value: '8' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ numPartitions: '8' }));

    const instructionsArea = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    expect(instructionsArea).toHaveValue('Initial SFTP guidance');
    fireEvent.change(instructionsArea, {
      target: { value: 'Parse ISO-8601 dates after CSV load' },
    });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Parse ISO-8601 dates after CSV load',
      })
    );
  });

  it('keeps non-secret SftpSource property keys safe and Secret Manager pointers classified as reference', () => {
    const safeKeys = [
      'host',
      'port',
      'username',
      'remotePath',
      'filePattern',
      'fileFormat',
      'csvHeader',
      'csvDelimiter',
      'jsonMultiLine',
      'xmlRowTag',
      'schemaDDL',
      'authType',
      'strictHostKeyChecking',
      'knownHostsPath',
      'numPartitions',
    ];
    for (const key of safeKeys) {
      expect(classifyKey(key), key).toBe('safe');
    }
    expect(classifyKey('authMode')).toBe('reference');
    expect(classifyKey('secretName')).toBe('reference');
  });
});
