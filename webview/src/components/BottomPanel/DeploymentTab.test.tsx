/**
 * @fileoverview Unit tests for DeploymentTab component (R1, R7, R12).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { DeploymentTab, buildDefaultCommandPreview, type LogEntry } from './DeploymentTab';
import { BottomPanelProvider } from './BottomPanelContext';
import { createBottomPanelContextValue, renderWithBottomPanel } from './testUtils';

describe('DeploymentTab', () => {
  const defaultProps = {
    jobConfig: {
      runner: 'ServerlessSpark',
      runtimeVersion: '4.0',
      tier: 'standard',
      depsBucket: 'gs://my-spark-deps',
      subnetwork: 'default',
      jobId: 'batch-2026-001',
      region: 'us-central1',
      projectId: 'my-test-project',
    },
    setJobConfig: vi.fn(),
    runtimeVariables: [
      { key: 'JOB_ID', value: 'job-123' },
      { key: 'API_SECRET_KEY', value: 'secret-12345' },
    ],
    addVariable: vi.fn(),
    updateVariable: vi.fn(),
    deleteVariable: vi.fn(),
    isJobIdValid: vi.fn().mockReturnValue(true),
    onRunNow: vi.fn(),
    onStopPipeline: vi.fn(),
    subnetworks: [{ name: 'sub-1', region: 'us-central1', network: 'default', value: 'sub-1' }],
    needsRebuild: false,
    pipelineLogs: [
      { id: '1', text: '[INFO] Pipeline starting...', stream: 'stdout', timestamp: '12:00:00' } as LogEntry,
      { id: '2', text: '[ERROR] Something failed', stream: 'stderr', timestamp: '12:00:01' } as LogEntry,
    ],
    pipelineStatus: 'idle' as const,
    runnerCommand: '',
    onClearLogs: vi.fn(),
    onOpenConsole: vi.fn(),
    projectId: 'my-test-project',
  };

  it('renders RUN PIPELINE button and does NOT render SCHEDULE button', () => {
    renderWithBottomPanel(<DeploymentTab />, defaultProps);

    expect(screen.getByRole('button', { name: /RUN PIPELINE/i })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /SCHEDULE/i })).not.toBeInTheDocument();
  });

  it('calls onRunNow when RUN PIPELINE button is clicked', () => {
    renderWithBottomPanel(<DeploymentTab />, defaultProps);

    const runButton = screen.getByRole('button', { name: /RUN PIPELINE/i });
    fireEvent.click(runButton);

    expect(defaultProps.onRunNow).toHaveBeenCalledTimes(1);
  });

  it('renders STOP button and cancel remote job checkbox for remote runners (R12)', () => {
    const onStopPipeline = vi.fn();
    renderWithBottomPanel(<DeploymentTab />, { ...defaultProps, onStopPipeline });

    const cancelCheckbox = screen.getByLabelText(/Cancel remote Dataproc job on Stop/i);
    expect(cancelCheckbox).toBeInTheDocument();
    expect(cancelCheckbox).toBeChecked();

    const stopButton = screen.getByRole('button', { name: /STOP/i });
    expect(stopButton).toBeInTheDocument();
    expect(stopButton.className).toContain('w-1/4');

    fireEvent.click(stopButton);
    expect(onStopPipeline).toHaveBeenCalledTimes(1);
    expect(onStopPipeline).toHaveBeenCalledWith({ cancelRemoteJob: true });

    // Uncheck remote cancellation and click STOP again
    fireEvent.click(cancelCheckbox);
    fireEvent.click(stopButton);
    expect(onStopPipeline).toHaveBeenLastCalledWith({ cancelRemoteJob: false });
  });

  it('previews ./run_local.sh, ./run_dataproc_cluster.sh, and ./run_serverless_spark.sh commands (R12)', () => {
    expect(
      buildDefaultCommandPreview('Local', { runner: 'Local' }, 'proj-1', 'us-central1')
    ).toContain('./run_local.sh');
    expect(
      buildDefaultCommandPreview(
        'DataprocCluster',
        { runner: 'DataprocCluster', clusterName: 'spark-cluster-a' },
        'proj-1',
        'us-central1'
      )
    ).toContain('CLUSTER_NAME=spark-cluster-a ./run_dataproc_cluster.sh');
    expect(
      buildDefaultCommandPreview(
        'ServerlessSpark',
        { runner: 'ServerlessSpark', depsBucket: 'gs://my-deps' },
        'proj-1',
        'us-central1'
      )
    ).toContain('DEPS_BUCKET=gs://my-deps ./run_serverless_spark.sh');
  });

  it('renders Execution Sandbox with runner command and logs', () => {
    renderWithBottomPanel(<DeploymentTab />, {
      ...defaultProps,
      runnerCommand: 'bash ./run_serverless_spark.sh',
    });

    expect(screen.getByText(/EXECUTION SANDBOX/i)).toBeInTheDocument();
    expect(screen.getByText(/RUNNER COMMAND/i)).toBeInTheDocument();
    expect(screen.getByText(/bash \.\/run_serverless_spark\.sh/i)).toBeInTheDocument();
    expect(screen.getByText(/\[INFO\] Pipeline starting\.\.\./i)).toBeInTheDocument();
    expect(screen.getByText(/\[ERROR\] Something failed/i)).toBeInTheDocument();
  });

  it('renders GCP Cloud Console monitoring link for ServerlessSpark and DataprocCluster jobs (R7)', () => {
    const onOpenConsole = vi.fn();
    const { rerenderWithContext } = renderWithBottomPanel(<DeploymentTab />, {
      ...defaultProps,
      onOpenConsole,
    });

    const consoleButton = screen.getByRole('button', { name: /Open in GCP Console/i });
    expect(consoleButton).toBeInTheDocument();

    fireEvent.click(consoleButton);
    expect(onOpenConsole).toHaveBeenCalledWith(
      'https://console.cloud.google.com/dataproc/batches/us-central1/batch-2026-001/monitoring?project=my-test-project'
    );

    rerenderWithContext(<DeploymentTab />, {
      ...defaultProps,
      onOpenConsole,
      jobConfig: {
        runner: 'DataprocCluster',
        clusterName: 'my-cluster',
        jobId: 'job-cluster-777',
        region: 'us-east1',
        projectId: 'my-test-project',
      },
    });

    fireEvent.click(screen.getByRole('button', { name: /Open in GCP Console/i }));
    expect(onOpenConsole).toHaveBeenLastCalledWith(
      'https://console.cloud.google.com/dataproc/jobs/job-cluster-777?region=us-east1&project=my-test-project'
    );
  });

  it('toggles secret visibility on eye icon click', () => {
    renderWithBottomPanel(<DeploymentTab />, defaultProps);

    const secretInput = screen.getByDisplayValue('secret-12345');
    expect(secretInput).toHaveAttribute('type', 'password');

    const toggleButton = screen.getByTitle(/Show secret value/i);
    fireEvent.click(toggleButton);

    expect(secretInput).toHaveAttribute('type', 'text');
  });

  it('calls onClearLogs when Clear button is clicked', () => {
    renderWithBottomPanel(<DeploymentTab />, defaultProps);

    const clearButton = screen.getByRole('button', { name: /Clear/i });
    fireEvent.click(clearButton);

    expect(defaultProps.onClearLogs).toHaveBeenCalledTimes(1);
  });

  it('calls setJobConfig with an updater function when Runtime Version changes and preserves jobId', () => {
    const setJobConfig = vi.fn();
    renderWithBottomPanel(<DeploymentTab />, { ...defaultProps, setJobConfig });

    const runtimeVersionSelect = screen.getByLabelText(/Runtime Version/i);
    fireEvent.change(runtimeVersionSelect, { target: { value: '3.0' } });

    expect(setJobConfig).toHaveBeenCalledTimes(1);
    const updater = setJobConfig.mock.calls[0][0];
    expect(
      typeof updater,
      'setJobConfig must receive an updater function so the merge uses the freshest state'
    ).toBe('function');

    const prevState = {
      runner: 'ServerlessSpark',
      runtimeVersion: '4.0',
      jobId: 'batch-abc',
      region: 'us-central1',
      projectId: 'my-project',
    };
    const nextState = updater(prevState);
    expect(nextState.runtimeVersion).toBe('3.0');
    expect(nextState.jobId).toBe('batch-abc');
    expect(nextState.region).toBe('us-central1');
    expect(nextState.projectId).toBe('my-project');
  });

  it('defaults Target Environment to Local when runner is unset or unrecognized (R1)', () => {
    renderWithBottomPanel(<DeploymentTab />, {
      ...defaultProps,
      jobConfig: { region: 'us-central1' },
    });

    const runnerSelect = screen.getByLabelText(/Target Environment/i);
    expect(runnerSelect).toHaveValue('Local');
  });

  describe('Spark Declarative Pipelines (SDP3 & SDP9) mode', () => {
    const sdpNodes = [
      {
        id: 'n-bronze',
        type: 'source',
        data: {
          label: 'Bronze Orders',
          nodeType: 'SOURCE',
          sparkTransform: 'CsvSource',
          sdpDatasetName: 'bronze_orders',
          sdpKind: 'streaming_table',
          sdpBadge: 'TABLE',
          properties: { readMode: 'streaming', sdpResetAllowed: false },
        },
      },
      {
        id: 'n-cdc',
        type: 'step',
        data: {
          label: 'Customer Auto CDC',
          nodeType: 'STEP',
          sparkTransform: 'MergeCdcScd',
          sdpDatasetName: 'silver_customers',
          sdpKind: 'auto_cdc',
          sdpBadge: 'CDC',
          properties: { scdType: 'scd1' },
        },
      },
      {
        id: 'n-mv',
        type: 'step',
        data: {
          label: 'Gold Revenue MV',
          nodeType: 'STEP',
          sparkTransform: 'GroupByAgg',
          sdpDatasetName: 'gold_revenue',
          sdpKind: 'materialized_view',
          sdpBadge: 'MV',
          properties: {},
        },
      },
      {
        id: 'n-sink',
        type: 'sink',
        data: {
          label: 'Kafka Sink',
          nodeType: 'SINK',
          sparkTransform: 'KafkaSink',
          sdpDatasetName: 'kafka_alerts_sink',
          sdpKind: 'external_sink',
          sdpBadge: 'SINK',
          properties: {},
        },
      },
    ];

    it('previews SDP runner commands (./run_sdp_local.sh, ./run_sdp_dataproc.sh, sdp_stage.pipeline.yaml) with refresh flags', () => {
      expect(
        buildDefaultCommandPreview(
          'Local',
          {
            runner: 'Local',
            sdpCommand: 'run',
            sdpRefreshMode: 'refresh',
            sdpRefreshDatasets: ['silver_customers', 'gold_revenue'],
          },
          'proj-1',
          'us-central1',
          'sdp'
        )
      ).toContain('SDP_REFRESH=silver_customers,gold_revenue ./run_sdp_local.sh');

      expect(
        buildDefaultCommandPreview(
          'DataprocCluster',
          {
            runner: 'DataprocCluster',
            clusterName: 'dp-40-cluster',
            sdpCommand: 'run',
            sdpRefreshMode: 'full_refresh_all',
          },
          'proj-1',
          'us-central1',
          'sdp'
        )
      ).toContain(
        'CLUSTER_NAME=dp-40-cluster SDP_FULL_REFRESH_ALL=true ./run_sdp_dataproc.sh'
      );

      expect(
        buildDefaultCommandPreview(
          'ServerlessSpark',
          { runner: 'ServerlessSpark', sdpCommand: 'dry-run' },
          'proj-1',
          'us-central1',
          'sdp'
        )
      ).toContain('sdp_stage.pipeline.yaml');
    });

    it('requires explicit confirmation naming every reset dataset and Auto CDC state table before running full_refresh_all', () => {
      const onRunNow = vi.fn();
      const baseCtx = createBottomPanelContextValue({
        ...defaultProps,
        onRunNow,
        nodes: sdpNodes,
        jobConfig: {
          ...defaultProps.jobConfig,
          sdpCommand: 'run',
          sdpRefreshMode: 'full_refresh_all',
        },
      });
      render(
        <BottomPanelProvider
          value={{
            ...baseCtx,
            pipelineConfig: {
              ...baseCtx.pipelineConfig,
              config: { ...baseCtx.pipelineConfig.config, paradigm: 'sdp' },
            },
          }}
        >
          <DeploymentTab />
        </BottomPanelProvider>
      );

      // MV truncation note and external sink duplicate warning are shown
      expect(screen.getByTestId('sdp-mv-truncation-note')).toHaveTextContent('gold_revenue');
      expect(screen.getByTestId('sdp-external-sink-duplicate-warning')).toHaveTextContent(
        'kafka_alerts_sink'
      );

      // Clicking RUN PIPELINE opens confirmation modal instead of calling onRunNow immediately
      fireEvent.click(screen.getByRole('button', { name: /RUN PIPELINE/i }));
      expect(onRunNow).not.toHaveBeenCalled();

      const modal = screen.getByTestId('sdp-full-refresh-confirm-modal');
      expect(modal).toBeInTheDocument();

      const resetList = screen.getByTestId('sdp-reset-dataset-list');
      // bronze_orders has sdpResetAllowed: false, so it is protected from reset, while resettable datasets and external sinks are named
      expect(resetList).not.toHaveTextContent('bronze_orders');
      expect(resetList).toHaveTextContent('silver_customers');
      expect(resetList).toHaveTextContent('__spark_autocdc_aux_state_silver_customers');
      expect(resetList).toHaveTextContent('gold_revenue');
      expect(resetList).toHaveTextContent('kafka_alerts_sink');
      expect(modal).toHaveTextContent(/only from source data still available/i);

      // Cancel closes modal without running
      fireEvent.click(screen.getByTestId('cancel-sdp-full-refresh-btn'));
      expect(
        screen.queryByTestId('sdp-full-refresh-confirm-modal')
      ).not.toBeInTheDocument();
      expect(onRunNow).not.toHaveBeenCalled();

      // Re-open and confirm -> calls onRunNow
      fireEvent.click(screen.getByRole('button', { name: /RUN PIPELINE/i }));
      fireEvent.click(screen.getByTestId('confirm-sdp-full-refresh-btn'));
      expect(onRunNow).toHaveBeenCalledTimes(1);
    });

    it('falls back to buildSdpPlan and data.type when React Flow type="pipeline" nodes omit sdpDatasetName', () => {
      const rawNodes = [
        {
          id: 'src-raw',
          type: 'pipeline',
          data: {
            label: 'Raw Orders Stream',
            type: 'SOURCE',
            spark_transform: 'CsvSource',
            properties: {
              readMode: 'streaming',
              filePattern: 'gs://raw-orders-bucket/orders/*.csv',
            },
          },
        },
        {
          id: 'agg-raw',
          type: 'pipeline',
          data: {
            label: 'Daily Totals',
            type: 'STEP',
            spark_transform: 'GroupByAgg',
            properties: {},
          },
        },
        {
          id: 'snk-raw',
          type: 'pipeline',
          data: {
            label: 'Outbound Events Sink',
            type: 'SINK',
            spark_transform: 'KafkaSink',
            properties: {},
          },
        },
      ];

      const baseCtx = createBottomPanelContextValue({
        ...defaultProps,
        nodes: rawNodes,
        jobConfig: {
          ...defaultProps.jobConfig,
          sdpStorage: 'gs://raw-orders-bucket/orders/checkpoints',
          sdpCommand: 'run',
          sdpRefreshMode: 'full_refresh_all',
        },
      });

      render(
        <BottomPanelProvider
          value={{
            ...baseCtx,
            pipelineConfig: {
              ...baseCtx.pipelineConfig,
              config: { ...baseCtx.pipelineConfig.config, paradigm: 'sdp' },
            },
          }}
        >
          <DeploymentTab />
        </BottomPanelProvider>
      );

      // Source folder overlap warning works on real React Flow type="pipeline" SOURCE node
      expect(screen.getByTestId('sdp-storage-error')).toHaveTextContent(
        /cannot equal or be nested inside source path/i
      );
      // External sink duplicate warning and MV truncation note resolve from React Flow type="pipeline" nodes
      expect(screen.getByTestId('sdp-external-sink-duplicate-warning')).toHaveTextContent(
        'outbound_events_sink'
      );
      expect(screen.getByTestId('sdp-mv-truncation-note')).toHaveTextContent('daily_totals');
    });
  });
});
