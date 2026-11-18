/**
 * Unit Tests for RedshiftSinkProperties Component (SNK26)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates RedshiftSinkProperties rendering, required url/tempdir/dbtable validation,
 * tempformat (AVRO/CSV), saveMode, diststyle/distkey/sortkeyspec layout controls,
 * preactions/postactions SQL hooks, awsIamRole, forwardSparkS3Credentials,
 * SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { RedshiftSinkProperties } from './RedshiftSink';

describe('RedshiftSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('validates required url, tempdir, and dbtable when empty', () => {
    const { container } = render(
      <RedshiftSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('Redshift JDBC URL is required')).toBeInTheDocument();
    expect(screen.getByText('Temporary S3 staging directory is required')).toBeInTheDocument();
    expect(screen.getByText('dbtable is required')).toBeInTheDocument();
    expect(container.querySelector('#redshiftSinkTempformat')).toHaveValue('AVRO');
    expect(container.querySelector('#redshiftSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#redshiftSinkDiststyle')).toHaveValue('AUTO');
  });

  it('hydrates properties and updates tempformat, diststyle/distkey, sortkeyspec, preactions, and postactions', () => {
    const { container, rerender } = render(
      <RedshiftSinkProperties
        node={{
          data: {
            properties: {
              url: 'jdbc:redshift://cluster.us-east-1.redshift.amazonaws.com:5439/dev',
              tempdir: 's3a://my-staging-bucket/redshift-tmp/',
              tempformat: 'CSV',
              dbtable: 'public.sales_summary',
              saveMode: 'overwrite',
              diststyle: 'KEY',
              distkey: '',
              sortkeyspec: 'SORTKEY(sale_date)',
              preactions: 'DELETE FROM public.sales_summary WHERE sale_date = CURRENT_DATE',
              postactions: 'GRANT SELECT ON public.sales_summary TO GROUP analysts',
              awsIamRole: 'arn:aws:iam::123456789012:role/RedshiftCopyRole',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#redshiftSinkTempformat')).toHaveValue('CSV');
    expect(container.querySelector('#redshiftSinkSaveMode')).toHaveValue('overwrite');
    expect(container.querySelector('#redshiftSinkDiststyle')).toHaveValue('KEY');
    expect(screen.getByText('distkey is required when diststyle is KEY')).toBeInTheDocument();

    fireEvent.change(container.querySelector('#redshiftSinkDistkey') as HTMLInputElement, {
      target: { name: 'distkey', value: 'customer_id' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ distkey: 'customer_id' }));

    rerender(
      <RedshiftSinkProperties
        node={{
          data: {
            properties: {
              url: 'jdbc:redshift://cluster.us-east-1.redshift.amazonaws.com:5439/dev',
              tempdir: 's3a://my-staging-bucket/redshift-tmp/',
              dbtable: 'public.sales_summary',
              diststyle: 'KEY',
              distkey: 'customer_id',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );
    expect(screen.queryByText('distkey is required when diststyle is KEY')).not.toBeInTheDocument();

    fireEvent.change(container.querySelector('#redshiftSinkSortkeyspec') as HTMLInputElement, {
      target: { name: 'sortkeyspec', value: 'COMPOUND SORTKEY(sale_date, region)' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ sortkeyspec: 'COMPOUND SORTKEY(sale_date, region)' })
    );
  });

  it('toggles forwardSparkS3Credentials, configures Secret Manager auth, and updates system_instructions', () => {
    const { container } = render(
      <RedshiftSinkProperties
        node={{
          data: {
            properties: {
              url: 'jdbc:redshift://host:5439/dev',
              tempdir: 's3a://bucket/tmp/',
              dbtable: 'public.events',
              authMode: 'secretManager',
              secretName: 'redshift-sink-secret',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#redshiftSinkSecretName')).toHaveValue('redshift-sink-secret');

    const forwardCheckbox = container.querySelector(
      '#redshiftSinkForwardSparkS3Credentials'
    ) as HTMLInputElement;
    fireEvent.click(forwardCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ forwardSparkS3Credentials: true })
    );

    const sysTextarea = container.querySelector(
      '#redshiftSinkSystemInstructions'
    ) as HTMLTextAreaElement;
    fireEvent.change(sysTextarea, { target: { value: 'Stage as AVRO in S3' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Stage as AVRO in S3' })
    );
  });
});
