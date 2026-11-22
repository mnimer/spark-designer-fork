/**
 * Unit Tests for JdbcIOSinkProperties Component (SNK24)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates JdbcIOSinkProperties rendering, database engine presets, SQL Server TLS banner,
 * saveMode overwrite truncate/cascadeTruncate toggles, batchsize default 5000, numPartitions,
 * isolationLevel, createTableOptions, SecretManagerAuthFields, legacy alias cleanup, and
 * system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { JdbcIOSinkProperties, JdbcSinkProperties } from './JdbcIO';

describe('JdbcIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports JdbcSinkProperties alias and validates required jdbcUrl and table when empty', () => {
    expect(JdbcSinkProperties).toBe(JdbcIOSinkProperties);

    const { container } = render(
      <JdbcIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('JDBC Connection URL is required')).toBeInTheDocument();
    expect(screen.getByText('Destination table is required')).toBeInTheDocument();
    expect(container.querySelector('#jdbcSinkDriverClass')).toHaveValue('org.postgresql.Driver');
    expect(container.querySelector('#jdbcSinkBatchsize')).toHaveValue(5000);
    expect(container.querySelector('#jdbcSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#jdbcSinkIsolationLevel')).toHaveValue('READ_COMMITTED');
  });

  it('switches dbEngine preset, auto-populates driverClass, and displays SQL Server Conscrypt note', () => {
    const { container, rerender } = render(
      <JdbcIOSinkProperties
        node={{
          data: {
            properties: {
              dbEngine: 'postgresql',
              jdbcUrl: 'jdbc:postgresql://localhost:5432/analytics',
              table: 'public.orders_summary',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const engineSelect = container.querySelector('#jdbcSinkDbEngine') as HTMLSelectElement;
    fireEvent.change(engineSelect, { target: { name: 'dbEngine', value: 'sqlserver' } });

    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dbEngine: 'sqlserver',
        driverClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
      })
    );

    rerender(
      <JdbcIOSinkProperties
        node={{
          data: {
            properties: {
              dbEngine: 'sqlserver',
              driverClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
              jdbcUrl: 'jdbc:sqlserver://localhost:1433;databaseName=analytics',
              table: 'dbo.orders_summary',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText(/SQL Server TLS note: sets dataproc\.artifacts\.remove=conscrypt/i)
    ).toBeInTheDocument();
  });

  it('renders truncate and cascadeTruncate controls when saveMode is overwrite', () => {
    const { container, rerender } = render(
      <JdbcIOSinkProperties
        node={{
          data: {
            properties: {
              jdbcUrl: 'jdbc:postgresql://localhost:5432/db',
              table: 'public.daily_totals',
              saveMode: 'append',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#jdbcSinkTruncate')).not.toBeInTheDocument();

    const saveModeSelect = container.querySelector('#jdbcSinkSaveMode') as HTMLSelectElement;
    fireEvent.change(saveModeSelect, { target: { name: 'saveMode', value: 'overwrite' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ saveMode: 'overwrite' }));

    rerender(
      <JdbcIOSinkProperties
        node={{
          data: {
            properties: {
              jdbcUrl: 'jdbc:postgresql://localhost:5432/db',
              table: 'public.daily_totals',
              saveMode: 'overwrite',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const truncateCheckbox = container.querySelector('#jdbcSinkTruncate') as HTMLInputElement;
    const cascadeCheckbox = container.querySelector('#jdbcSinkCascadeTruncate') as HTMLInputElement;
    expect(truncateCheckbox).toBeChecked();
    expect(cascadeCheckbox).not.toBeChecked();

    fireEvent.click(cascadeCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ cascadeTruncate: true })
    );
  });

  it('updates batchsize, numPartitions, isolationLevel, createTableOptions, and driverJarUri', () => {
    const { container } = render(
      <JdbcIOSinkProperties
        node={{
          data: {
            properties: {
              dbEngine: 'mysql',
              jdbcUrl: 'jdbc:mysql://localhost:3306/db',
              table: 'events',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(container.querySelector('#jdbcSinkBatchsize') as HTMLInputElement, {
      target: { name: 'batchsize', value: '10000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ batchsize: '10000' }));

    fireEvent.change(container.querySelector('#jdbcSinkNumPartitions') as HTMLInputElement, {
      target: { name: 'numPartitions', value: '12' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ numPartitions: '12' }));

    fireEvent.change(container.querySelector('#jdbcSinkIsolationLevel') as HTMLSelectElement, {
      target: { name: 'isolationLevel', value: 'SERIALIZABLE' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ isolationLevel: 'SERIALIZABLE' })
    );

    fireEvent.change(container.querySelector('#jdbcSinkCreateTableOptions') as HTMLInputElement, {
      target: { name: 'createTableOptions', value: 'ENGINE=InnoDB' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ createTableOptions: 'ENGINE=InnoDB' })
    );

    fireEvent.change(container.querySelector('#jdbcSinkDriverJarUri') as HTMLInputElement, {
      target: { name: 'driverJarUri', value: 'gs://jars/mysql-connector.jar' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ driverJarUri: 'gs://jars/mysql-connector.jar' })
    );
  });

  it('hydrates legacy aliases, strips plaintext password on edit, and updates system_instructions', () => {
    const { container } = render(
      <JdbcIOSinkProperties
        node={{
          data: {
            system_instructions: 'Use connection pooling cap',
            properties: {
              driverClassName: 'oracle.jdbc.OracleDriver',
              url: 'jdbc:oracle:thin:@//localhost:1521/ORCL',
              tableName: 'FINANCE.LEDGER',
              batchSize: 2500,
              passwordSecret: 'oracle-pw-secret',
              password: 'legacy-cleartext-password',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#jdbcSinkDbEngine')).toHaveValue('oracle');
    expect(container.querySelector('#jdbcSinkDriverClass')).toHaveValue('oracle.jdbc.OracleDriver');
    expect(container.querySelector('#jdbcSinkJdbcUrl')).toHaveValue(
      'jdbc:oracle:thin:@//localhost:1521/ORCL'
    );
    expect(container.querySelector('#jdbcSinkTable')).toHaveValue('FINANCE.LEDGER');
    expect(container.querySelector('#jdbcSinkBatchsize')).toHaveValue(2500);
    expect(container.querySelector('#jdbcSinkSecretName')).toHaveValue('oracle-pw-secret');
    expect(container.querySelector('input[name="password"]')).not.toBeInTheDocument();

    fireEvent.change(container.querySelector('#jdbcSinkTable') as HTMLInputElement, {
      target: { name: 'table', value: 'FINANCE.LEDGER_V2' },
    });
    const updatedProps = mockOnChange.mock.calls[0][0];
    expect(updatedProps.table).toBe('FINANCE.LEDGER_V2');
    expect(updatedProps.tableName).toBeUndefined();
    expect(updatedProps.password).toBeUndefined();

    const sysTextarea = container.querySelector('#jdbcSinkSystemInstructions') as HTMLTextAreaElement;
    expect(sysTextarea).toHaveValue('Use connection pooling cap');
    fireEvent.change(sysTextarea, { target: { value: 'Set queryTimeout=60' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Set queryTimeout=60' })
    );
  });
});
