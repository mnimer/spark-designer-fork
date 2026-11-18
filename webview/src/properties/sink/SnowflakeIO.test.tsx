/**
 * Unit Tests for SnowflakeIOSinkProperties Component (SNK25)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates SnowflakeIOSinkProperties rendering, required sfURL/sfWarehouse/sfDatabase/dbtable
 * validation, saveMode and truncateTable controls, usestagingtable toggle, preactions and
 * postactions SQL hooks, password vs pem_private_key Secret Manager auth, legacy alias cleanup,
 * and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SnowflakeIOSinkProperties, SnowflakeSinkProperties } from './SnowflakeIO';

describe('SnowflakeIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports SnowflakeSinkProperties alias and validates required Snowflake sink fields when empty', () => {
    expect(SnowflakeSinkProperties).toBe(SnowflakeIOSinkProperties);

    const { container } = render(
      <SnowflakeIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('sfURL is required')).toBeInTheDocument();
    expect(screen.getByText('sfWarehouse is required')).toBeInTheDocument();
    expect(screen.getByText('sfDatabase is required')).toBeInTheDocument();
    expect(screen.getByText('dbtable is required')).toBeInTheDocument();
    expect(container.querySelector('#snowflakeSinkSfSchema')).toHaveValue('PUBLIC');
    expect(container.querySelector('#snowflakeSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#snowflakeSinkUseStagingTable')).toBeChecked();
  });

  it('hydrates Spark Snowflake options and dispatches updates for staging, preactions, and postactions', () => {
    const { container } = render(
      <SnowflakeIOSinkProperties
        node={{
          data: {
            properties: {
              sfURL: 'xy12345.us-central1.gcp.snowflakecomputing.com',
              sfWarehouse: 'COMPUTE_WH',
              sfDatabase: 'ANALYTICS_DB',
              sfSchema: 'MART',
              sfRole: 'DATA_ENGINEER',
              sfUser: 'SPARK_WRITER',
              dbtable: 'DAILY_REVENUE',
              saveMode: 'overwrite',
              usestagingtable: true,
              truncateTable: true,
              preactions: 'DELETE FROM DAILY_REVENUE WHERE DT = CURRENT_DATE()',
              postactions: 'GRANT SELECT ON TABLE DAILY_REVENUE TO ROLE BI_READER',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#snowflakeSinkSfUrl')).toHaveValue(
      'xy12345.us-central1.gcp.snowflakecomputing.com'
    );
    expect(container.querySelector('#snowflakeSinkSfWarehouse')).toHaveValue('COMPUTE_WH');
    expect(container.querySelector('#snowflakeSinkSfDatabase')).toHaveValue('ANALYTICS_DB');
    expect(container.querySelector('#snowflakeSinkSfSchema')).toHaveValue('MART');
    expect(container.querySelector('#snowflakeSinkSfRole')).toHaveValue('DATA_ENGINEER');
    expect(container.querySelector('#snowflakeSinkDbtable')).toHaveValue('DAILY_REVENUE');
    expect(container.querySelector('#snowflakeSinkTruncateTable')).toBeChecked();

    fireEvent.click(container.querySelector('#snowflakeSinkUseStagingTable') as HTMLInputElement);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ usestagingtable: false })
    );

    fireEvent.change(container.querySelector('#snowflakeSinkPreactions') as HTMLTextAreaElement, {
      target: { name: 'preactions', value: 'TRUNCATE TABLE STAGE_REV' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ preactions: 'TRUNCATE TABLE STAGE_REV' })
    );

    fireEvent.change(container.querySelector('#snowflakeSinkPostactions') as HTMLTextAreaElement, {
      target: { name: 'postactions', value: 'ALTER TABLE DAILY_REVENUE SWAP WITH STAGE_REV' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        postactions: 'ALTER TABLE DAILY_REVENUE SWAP WITH STAGE_REV',
      })
    );
  });

  it('supports switching credentialType between password and pem_private_key and cleans legacy aliases', () => {
    const { container } = render(
      <SnowflakeIOSinkProperties
        node={{
          data: {
            properties: {
              serverName: 'legacy.snowflakecomputing.com',
              warehouse: 'LEGACY_WH',
              database: 'LEGACY_DB',
              table: 'LEGACY_TBL',
              privateKeySecret: 'sf-rsa-key-secret',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#snowflakeSinkSfUrl')).toHaveValue(
      'legacy.snowflakecomputing.com'
    );
    expect(container.querySelector('#snowflakeSinkCredentialType')).toHaveValue('pem_private_key');
    expect(container.querySelector('#snowflakeSinkSecretName')).toHaveValue('sf-rsa-key-secret');

    fireEvent.change(container.querySelector('#snowflakeSinkDbtable') as HTMLInputElement, {
      target: { name: 'dbtable', value: 'CANONICAL_TBL' },
    });
    const updated = mockOnChange.mock.calls[0][0];
    expect(updated.dbtable).toBe('CANONICAL_TBL');
    expect(updated.table).toBeUndefined();

    fireEvent.change(container.querySelector('#snowflakeSinkSystemInstructions') as HTMLTextAreaElement, {
      target: { value: 'Use MERGE INTO postaction' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Use MERGE INTO postaction' })
    );
  });
});
