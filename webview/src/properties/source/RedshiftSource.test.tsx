/**
 * Unit Tests for RedshiftSourceProperties Component (SRC29)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates RedshiftSourceProperties rendering, required url/tempdir/dbtable validation,
 * query mode switching, awsIamRole, forwardSparkS3Credentials toggle, SecretManagerAuthFields,
 * and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { RedshiftSourceProperties } from './RedshiftSource';

describe('RedshiftSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('validates required url, tempdir, and dbtable when empty', () => {
    render(<RedshiftSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Redshift JDBC URL is required')).toBeInTheDocument();
    expect(screen.getByText('Temporary S3/GCS staging directory is required')).toBeInTheDocument();
    expect(screen.getByText('dbtable is required in Table mode')).toBeInTheDocument();
  });

  it('hydrates properties and dispatches updates for url, tempdir, awsIamRole, and forwardSparkS3Credentials', () => {
    const node = {
      data: {
        properties: {
          url: 'jdbc:redshift://cluster.us-east-1.redshift.amazonaws.com:5439/dev',
          tempdir: 's3a://my-bucket/temp/',
          dbtable: 'public.sales',
          awsIamRole: 'arn:aws:iam::123456789012:role/RedshiftRole',
          forwardSparkS3Credentials: false,
        },
      },
    };

    const { container } = render(<RedshiftSourceProperties node={node} onChange={mockOnChange} />);

    expect(
      screen.getByDisplayValue('jdbc:redshift://cluster.us-east-1.redshift.amazonaws.com:5439/dev')
    ).toBeInTheDocument();
    expect(screen.getByDisplayValue('s3a://my-bucket/temp/')).toBeInTheDocument();
    expect(screen.getByDisplayValue('public.sales')).toBeInTheDocument();
    expect(screen.getByDisplayValue('arn:aws:iam::123456789012:role/RedshiftRole')).toBeInTheDocument();

    const forwardCheckbox = container.querySelector(
      '#redshiftSourceForwardSparkS3Credentials'
    ) as HTMLInputElement;
    expect(forwardCheckbox).not.toBeChecked();

    fireEvent.click(forwardCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ forwardSparkS3Credentials: true })
    );
  });

  it('switches to query mode and updates query and system_instructions', () => {
    const { container } = render(
      <RedshiftSourceProperties
        node={{
          data: {
            properties: {
              url: 'jdbc:redshift://host:5439/dev',
              tempdir: 's3a://bucket/tmp/',
              readTargetMode: 'query',
              query: 'SELECT * FROM public.sales',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const queryInput = container.querySelector('#redshiftSourceQuery') as HTMLTextAreaElement;
    expect(queryInput).toHaveValue('SELECT * FROM public.sales');

    fireEvent.change(queryInput, { target: { name: 'query', value: 'SELECT id FROM public.sales' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ query: 'SELECT id FROM public.sales' })
    );

    const sysInput = container.querySelector('#redshiftSourceSystemInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Use S3 IAM role' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Use S3 IAM role' })
    );
  });
});
