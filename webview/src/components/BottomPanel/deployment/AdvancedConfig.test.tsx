/**
 * @fileoverview Unit tests for AdvancedConfig runner modes (R2, R3, R4) and streaming defaults (A9).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  AdvancedConfig,
  validateDepsBucketUri,
  type AdvancedJobConfig,
} from './AdvancedConfig';

describe('AdvancedConfig', () => {
  const sampleSubnetworks = [
    { name: 'spark-subnet', region: 'us-central1', network: 'default', value: 'spark-subnet' },
  ];

  describe('validateDepsBucketUri (R4)', () => {
    it('returns null for empty or valid root gs://<bucket> URIs', () => {
      expect(validateDepsBucketUri('')).toBeNull();
      expect(validateDepsBucketUri(undefined)).toBeNull();
      expect(validateDepsBucketUri('gs://my-spark-deps-bucket')).toBeNull();
      expect(validateDepsBucketUri('gs://my-spark-deps-bucket/')).toBeNull();
    });

    it('returns a validation warning when missing gs:// prefix or containing path segments', () => {
      expect(validateDepsBucketUri('my-bucket')).toMatch(/must start with gs:\/\//i);
      expect(validateDepsBucketUri('s3://my-bucket')).toMatch(/must start with gs:\/\//i);
      expect(validateDepsBucketUri('gs://')).toMatch(/root bucket name/i);
      expect(validateDepsBucketUri('gs://my-bucket/staging/deps')).toMatch(
        /without object path segments/i
      );
    });
  });

  describe('Local mode (R2)', () => {
    it('renders Use Spark Connect toggle and Runtime Profile (Session Template) input when runner is Local', () => {
      let state: AdvancedJobConfig = { runner: 'Local' };
      const setJobConfig = vi.fn((updater) => {
        state = typeof updater === 'function' ? updater(state) : updater;
      });

      const { rerender } = render(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
          sessionTemplates={[
            { name: 'serverless-interactive', value: 'projects/p/locations/us-central1/sessionTemplates/t1' },
          ]}
        />
      );

      expect(screen.getByTestId('local-runner-options')).toBeInTheDocument();

      const sparkConnectToggle = screen.getByLabelText(
        /Use Spark Connect \(ManagedSparkSession\)/i
      );
      expect(sparkConnectToggle).not.toBeChecked();

      fireEvent.click(sparkConnectToggle);
      expect(state.useSparkConnect).toBe(true);

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );
      expect(
        screen.getByLabelText(/Use Spark Connect \(ManagedSparkSession\)/i)
      ).toBeChecked();

      const profileInput = screen.getByLabelText(/Runtime Profile \(Session Template\)/i);
      fireEvent.change(profileInput, {
        target: { value: 'projects/my-proj/locations/us-central1/sessionTemplates/spark-4' },
      });
      expect(state.sessionTemplate).toBe(
        'projects/my-proj/locations/us-central1/sessionTemplates/spark-4'
      );
    });
  });

  describe('DataprocCluster mode (R3)', () => {
    it('renders Cluster Name, Region, Subnetwork, and Custom JAR URIs (gs://) controls', () => {
      let state: AdvancedJobConfig = { runner: 'DataprocCluster' };
      const setJobConfig = vi.fn((updater) => {
        state = typeof updater === 'function' ? updater(state) : updater;
      });

      const { rerender } = render(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
          clusters={[{ name: 'analytics-cluster-1', status: 'RUNNING', region: 'us-central1' }]}
        />
      );

      expect(screen.getByTestId('dataproc-cluster-options')).toBeInTheDocument();
      expect(screen.getByLabelText(/Region/i)).toHaveValue('us-central1');

      fireEvent.change(screen.getByLabelText(/Cluster Name/i), {
        target: { value: 'analytics-cluster-1' },
      });
      expect(state.clusterName).toBe('analytics-cluster-1');

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );

      fireEvent.change(screen.getByLabelText(/Region/i), {
        target: { value: 'europe-west1' },
      });
      expect(state.region).toBe('europe-west1');

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );

      fireEvent.change(screen.getByLabelText(/Subnetwork/i), {
        target: { value: 'spark-subnet' },
      });
      expect(state.subnetwork).toBe('spark-subnet');

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );

      fireEvent.change(screen.getByLabelText(/Custom JAR URIs/i), {
        target: { value: 'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar' },
      });
      expect(state.customJarUris).toBe('gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar');
      expect(state.sparkJars).toBe('gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar');
    });
  });

  describe('ServerlessSpark mode (R4)', () => {
    it('defaults Runtime Version to 4.0 and Tier to standard, and updates all Serverless Spark fields', () => {
      let state: AdvancedJobConfig = { runner: 'ServerlessSpark' };
      const setJobConfig = vi.fn((updater) => {
        state = typeof updater === 'function' ? updater(state) : updater;
      });

      const { rerender } = render(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );

      expect(screen.getByTestId('serverless-spark-options')).toBeInTheDocument();

      const versionSelect = screen.getByLabelText(/Runtime Version/i);
      const tierSelect = screen.getByLabelText(/Tier/i);
      expect(versionSelect).toHaveValue('4.0');
      expect(tierSelect).toHaveValue('standard');

      // Change Runtime Version to 3.0
      fireEvent.change(versionSelect, { target: { value: '3.0' } });
      expect(state.runtimeVersion).toBe('3.0');

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );

      // Change Tier to premium
      fireEvent.change(screen.getByLabelText(/Tier/i), { target: { value: 'premium' } });
      expect(state.tier).toBe('premium');
      expect(state.serverlessTier).toBe('premium');

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );

      // Enter invalid Deps Bucket with subpath and verify inline warning
      fireEvent.change(screen.getByLabelText(/Deps Bucket/i), {
        target: { value: 'gs://my-bucket/staging' },
      });
      expect(state.depsBucket).toBe('gs://my-bucket/staging');

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );
      expect(screen.getByTestId('deps-bucket-warning')).toHaveTextContent(
        /without object path segments/i
      );

      // Fix Deps Bucket to root bucket URI and verify warning disappears
      fireEvent.change(screen.getByLabelText(/Deps Bucket/i), {
        target: { value: 'gs://my-bucket' },
      });
      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );
      expect(screen.queryByTestId('deps-bucket-warning')).not.toBeInTheDocument();

      // Update Service Account and Batch TTL (minutes)
      fireEvent.change(screen.getByLabelText(/Service Account/i), {
        target: { value: 'spark-runner@my-proj.iam.gserviceaccount.com' },
      });
      expect(state.serviceAccount).toBe('spark-runner@my-proj.iam.gserviceaccount.com');

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );

      fireEvent.change(screen.getByLabelText(/Batch TTL \(minutes\)/i), {
        target: { value: '120' },
      });
      expect(state.batchTtlMinutes).toBe(120);
    });
  });

  describe('Streaming Defaults (A9)', () => {
    it('renders Streaming Defaults with processingTime and 10 seconds interval by default', () => {
      const jobConfig: AdvancedJobConfig = { runner: 'Spark' };
      render(
        <AdvancedConfig
          jobConfig={jobConfig}
          setJobConfig={vi.fn()}
          subnetworks={sampleSubnetworks}
        />
      );

      expect(screen.getByTestId('pipeline-streaming-defaults')).toBeInTheDocument();
      expect(screen.getByLabelText(/Checkpoint Root URI/i)).toHaveValue('');
      expect(screen.getByLabelText(/Default Trigger Type/i)).toHaveValue('processingTime');
      expect(screen.getByLabelText(/Default Trigger Interval/i)).toHaveValue('10 seconds');
    });

    it('updates checkpointRoot, defaultTriggerType, and defaultTriggerInterval via functional setJobConfig', () => {
      let state: AdvancedJobConfig = { runner: 'ServerlessSpark', region: 'us-central1' };
      const setJobConfig = vi.fn((updater) => {
        state = typeof updater === 'function' ? updater(state) : updater;
      });

      const { rerender } = render(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );

      fireEvent.change(screen.getByLabelText(/Checkpoint Root URI/i), {
        target: { value: 'gs://my-bucket/checkpoints' },
      });
      expect(state.checkpointRoot).toBe('gs://my-bucket/checkpoints');
      expect(state.region).toBe('us-central1');

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );

      fireEvent.change(screen.getByLabelText(/Default Trigger Interval/i), {
        target: { value: '30 seconds' },
      });
      expect(state.defaultTriggerInterval).toBe('30 seconds');

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );

      // Switching to availableNow hides Default Trigger Interval
      fireEvent.change(screen.getByLabelText(/Default Trigger Type/i), {
        target: { value: 'availableNow' },
      });
      expect(state.defaultTriggerType).toBe('availableNow');

      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );
      expect(screen.queryByLabelText(/Default Trigger Interval/i)).not.toBeInTheDocument();

      // Switching to once also hides Default Trigger Interval
      fireEvent.change(screen.getByLabelText(/Default Trigger Type/i), {
        target: { value: 'once' },
      });
      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );
      expect(screen.queryByLabelText(/Default Trigger Interval/i)).not.toBeInTheDocument();

      // Switching to continuous shows Default Trigger Interval
      fireEvent.change(screen.getByLabelText(/Default Trigger Type/i), {
        target: { value: 'continuous' },
      });
      rerender(
        <AdvancedConfig
          jobConfig={state}
          setJobConfig={setJobConfig}
          subnetworks={sampleSubnetworks}
        />
      );
      expect(screen.getByLabelText(/Default Trigger Interval/i)).toHaveValue('30 seconds');
    });
  });

  describe('Live GCP Asset Discovery integration (D1, D2, D5)', () => {
    it('requests Dataproc clusters and regions in DataprocCluster mode and updates datalists on host response', () => {
      const mockPostMessage = vi.fn();
      window.vscode = { postMessage: mockPostMessage };

      try {
        let state: AdvancedJobConfig = { runner: 'DataprocCluster', region: 'us-central1' };
        const setJobConfig = vi.fn((updater) => {
          state = typeof updater === 'function' ? updater(state) : updater;
        });

        const { container, rerender } = render(
          <AdvancedConfig
            jobConfig={state}
            setJobConfig={setJobConfig}
            subnetworks={sampleSubnetworks}
          />
        );

        expect(mockPostMessage).toHaveBeenCalledWith({
          command: 'getDataprocClusters',
          region: 'us-central1',
        });
        expect(mockPostMessage).toHaveBeenCalledWith({
          command: 'getRegions',
        });

        // Simulate host responding with clusters and regions
        fireEvent(
          window,
          new MessageEvent('message', {
            data: {
              command: 'dataprocClustersLoaded',
              clusters: [{ name: 'live-cluster-1', status: 'RUNNING', region: 'us-central1' }],
              requestParams: { region: 'us-central1' },
            },
          })
        );
        fireEvent(
          window,
          new MessageEvent('message', {
            data: {
              command: 'regionsLoaded',
              regions: ['us-central1', 'northamerica-northeast1'],
            },
          })
        );

        const clusterDatalist = container.querySelector('#advancedDataprocClustersList');
        expect(clusterDatalist?.querySelector('option[value="live-cluster-1"]')).toBeInTheDocument();

        const regionDatalist = container.querySelector('#advancedClusterRegionsList');
        expect(
          regionDatalist?.querySelector('option[value="northamerica-northeast1"]')
        ).toBeInTheDocument();

        // Changing region re-triggers getDataprocClusters for the new region
        mockPostMessage.mockClear();
        state = { ...state, region: 'europe-west1' };
        rerender(
          <AdvancedConfig
            jobConfig={state}
            setJobConfig={setJobConfig}
            subnetworks={sampleSubnetworks}
          />
        );
        expect(mockPostMessage).toHaveBeenCalledWith({
          command: 'getDataprocClusters',
          region: 'europe-west1',
        });
      } finally {
        delete window.vscode;
      }
    });

    it('requests storageBuckets in ServerlessSpark mode and populates root gs://<bucket> datalist', () => {
      const mockPostMessage = vi.fn();
      window.vscode = { postMessage: mockPostMessage };

      try {
        const state: AdvancedJobConfig = { runner: 'ServerlessSpark' };
        const { container } = render(
          <AdvancedConfig
            jobConfig={state}
            setJobConfig={vi.fn()}
            subnetworks={sampleSubnetworks}
          />
        );

        expect(mockPostMessage).toHaveBeenCalledWith({
          command: 'getStorageBuckets',
        });

        fireEvent(
          window,
          new MessageEvent('message', {
            data: {
              command: 'storageBucketsLoaded',
              buckets: ['spark-deps-prod', 'gs://already-prefixed/'],
            },
          })
        );

        const bucketDatalist = container.querySelector('#advancedDepsBucketsList');
        expect(
          bucketDatalist?.querySelector('option[value="gs://spark-deps-prod"]')
        ).toBeInTheDocument();
        expect(
          bucketDatalist?.querySelector('option[value="gs://already-prefixed"]')
        ).toBeInTheDocument();
      } finally {
        delete window.vscode;
      }
    });
  });

  describe('Spark Declarative Pipelines (SDP3) mode', () => {
    it('hides imperative Streaming Defaults, renders SdpSettingsPanel, and flags runtime 3.0 on ServerlessSpark', () => {
      render(
        <AdvancedConfig
          jobConfig={{ runner: 'ServerlessSpark', runtimeVersion: '3.0' }}
          setJobConfig={vi.fn()}
          subnetworks={sampleSubnetworks}
          paradigm="sdp"
        />
      );

      expect(screen.queryByTestId('pipeline-streaming-defaults')).not.toBeInTheDocument();
      expect(screen.getByTestId('sdp-settings-panel')).toBeInTheDocument();
      expect(screen.getByTestId('sdp-runtime-version-warning')).toHaveTextContent(
        /SDP_SPARK_42_REQUIRED/
      );
    });

    it('renders Dataproc 4.0 ICEBERG and zstandard>=0.25.0 note in SDP mode', () => {
      render(
        <AdvancedConfig
          jobConfig={{ runner: 'DataprocCluster', runtimeVersion: '4.0' }}
          setJobConfig={vi.fn()}
          subnetworks={sampleSubnetworks}
          paradigm="sdp"
        />
      );

      const note = screen.getByTestId('sdp-dataproc-cluster-note');
      expect(note).toHaveTextContent('--optional-components=ICEBERG');
      expect(note).toHaveTextContent('zstandard>=0.25.0');
    });

    it('shows missing local iceberg-spark-runtime-4.2_2.13 warning banner when Local SDP has Iceberg nodes', () => {
      const icebergNodes = [
        {
          id: 'snk-1',
          type: 'sink',
          data: { nodeType: 'SINK', sparkTransform: 'IcebergSink', properties: {} },
        },
      ];

      const { rerender } = render(
        <AdvancedConfig
          jobConfig={{ runner: 'Local', runtimeVersion: '4.2-connect' }}
          setJobConfig={vi.fn()}
          subnetworks={sampleSubnetworks}
          paradigm="sdp"
          nodes={icebergNodes}
        />
      );

      expect(screen.getByTestId('sdp-local-connect-info')).toBeInTheDocument();
      const warning = screen.getByTestId('sdp-local-iceberg-warning');
      expect(warning).toHaveTextContent('iceberg-spark-runtime-4.2_2.13');
      expect(warning).toHaveTextContent('org.apache.iceberg.spark.SparkCatalog');
      expect(warning).toHaveTextContent('./run_sdp_local.sh --test');
      expect(warning).toHaveTextContent('SPARK_JARS');

      // Warning disappears when no Iceberg nodes are on the canvas
      rerender(
        <AdvancedConfig
          jobConfig={{ runner: 'Local', runtimeVersion: '4.2-connect' }}
          setJobConfig={vi.fn()}
          subnetworks={sampleSubnetworks}
          paradigm="sdp"
          nodes={[]}
        />
      );
      expect(screen.queryByTestId('sdp-local-iceberg-warning')).not.toBeInTheDocument();
    });
  });
});
