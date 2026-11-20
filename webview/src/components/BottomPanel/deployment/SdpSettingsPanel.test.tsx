/**
 * @fileoverview Unit tests for SdpSettingsPanel component (SDP3).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SdpSettingsPanel } from './SdpSettingsPanel';

describe('SdpSettingsPanel (SDP3)', () => {
  it('renders defaults (catalog "lake", lakehouse mode, createNamespace true) and updates via functional setJobConfig updaters', () => {
    let state: Record<string, unknown> = {
      runner: 'ServerlessSpark',
      region: 'us-central1',
    };
    const setJobConfig = vi.fn((updater) => {
      state = typeof updater === 'function' ? updater(state) : updater;
    });

    const { rerender } = render(
      <SdpSettingsPanel
        jobConfig={state}
        setJobConfig={setJobConfig}
        projectId="analytics-prod"
      />
    );

    expect(screen.getByTestId('sdp-settings-panel')).toBeInTheDocument();
    expect(screen.getByLabelText(/Catalog Alias/i, { selector: 'input' })).toHaveValue('lake');
    expect(screen.getByLabelText(/BigLake Catalog Mode/i, { selector: 'select' })).toHaveValue(
      'lakehouse'
    );
    expect(
      screen.getByLabelText(/Create Target Namespace Before Run/i, { selector: 'input' })
    ).toBeChecked();

    fireEvent.change(screen.getByLabelText(/Pipeline Storage URI/i, { selector: 'input' }), {
      target: { value: 'gs://sdp-checkpoints/medallion' },
    });
    expect(typeof setJobConfig.mock.calls[0][0]).toBe('function');
    expect(state.sdpStorage).toBe('gs://sdp-checkpoints/medallion');
    expect(state.region).toBe('us-central1');

    rerender(
      <SdpSettingsPanel
        jobConfig={state}
        setJobConfig={setJobConfig}
        projectId="analytics-prod"
      />
    );
    expect(screen.queryByTestId('sdp-storage-error')).not.toBeInTheDocument();
  });

  it('displays validation alerts for bare storage paths, source folder overlaps, hyphenated catalog aliases, and gcs_bucket /warehouse suffixes', () => {
    const sourceNodes = [
      {
        id: 'src-1',
        type: 'pipeline',
        data: {
          type: 'SOURCE',
          spark_transform: 'CsvSource',
          properties: { filePattern: 'gs://landing-bucket/orders/*.csv' },
        },
      },
    ];

    const { rerender } = render(
      <SdpSettingsPanel
        jobConfig={{
          sdpStorage: 'gs://landing-bucket/orders/checkpoints',
          sdpCatalog: 'my-hyphenated-catalog',
          sdpBigLakeMode: 'gcs_bucket',
          sdpBigLakeProject: 'my-proj',
          sdpBigLakeWarehouseBucket: 'gs://iceberg-bucket/warehouse',
        }}
        setJobConfig={vi.fn()}
        nodes={sourceNodes}
      />
    );

    expect(screen.getByTestId('sdp-storage-error')).toHaveTextContent(
      /cannot equal or be nested inside source path/i
    );
    expect(screen.getByTestId('sdp-catalog-error')).toHaveTextContent(/INVALID_IDENTIFIER/);
    expect(screen.getByTestId('sdp-biglake-warehouse-error')).toHaveTextContent(
      /BadRequestException: Bucket name is invalid/
    );

    // Fix to valid sibling storage, plain catalog alias, and root bucket warehouse
    rerender(
      <SdpSettingsPanel
        jobConfig={{
          sdpStorage: 'gs://landing-bucket/orders_checkpoints',
          sdpCatalog: 'lake',
          sdpBigLakeMode: 'gcs_bucket',
          sdpBigLakeProject: 'my-proj',
          sdpBigLakeWarehouseBucket: 'gs://iceberg-bucket',
        }}
        setJobConfig={vi.fn()}
        nodes={sourceNodes}
      />
    );

    expect(screen.queryByTestId('sdp-storage-error')).not.toBeInTheDocument();
    expect(screen.queryByTestId('sdp-catalog-error')).not.toBeInTheDocument();
    expect(screen.queryByTestId('sdp-biglake-warehouse-error')).not.toBeInTheDocument();
  });

  it('displays live routing notice when static Spark keys are entered in Spark & Execution Properties', () => {
    let state: Record<string, unknown> = {
      sdpCatalog: 'lake',
      sdpBigLakeMode: 'lakehouse',
      sdpBigLakeProject: 'my-proj',
      sdpBigLakeCatalogId: 'retail_cat',
    };
    const setJobConfig = vi.fn((updater) => {
      state = typeof updater === 'function' ? updater(state) : updater;
    });

    const { rerender } = render(
      <SdpSettingsPanel jobConfig={state} setJobConfig={setJobConfig} />
    );

    const textarea = screen.getByLabelText(/Spark & Execution Properties/i, {
      selector: 'textarea',
    });
    fireEvent.change(textarea, {
      target: {
        value:
          'spark.sql.shuffle.partitions=200\nspark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\nspark.driver.memory=8g',
      },
    });

    rerender(<SdpSettingsPanel jobConfig={state} setJobConfig={setJobConfig} />);

    const notice = screen.getByTestId('sdp-static-config-routing-notice');
    expect(notice).toHaveTextContent('spark.sql.extensions');
    expect(notice).toHaveTextContent('spark.driver.memory');
    expect(notice).toHaveTextContent('CANNOT_MODIFY_STATIC_CONFIG');
  });
});
