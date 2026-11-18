/**
 * PropertyForm Unit Tests
 * 
 * Author: Mike Nimer <mikenimer@google.com>
 * 
 * Purpose:
 * Validates immutable metadata-based property form resolution, regression resistance against
 * label changes, fallback handling for unknown or missing metadata, and coverage across all
 * registered Beam transforms and IO components.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import fs from 'fs';
import path from 'path';
import YAML from 'yaml';
import { PropertyForm, PROPERTY_FORM_REGISTRY, getPropertyFormComponent, loadPropertyForm } from './PropertyForm';
import { migrateLegacyNodes } from '../utils/legacyNodeMigration';
import { getNodeDocumentation } from '../utils/nodeDocumentation';

import { PubSubProperties } from '../properties/source/PubSubIO';
import { BigQuerySourceProperties } from '../properties/source/BigQueryIO';
import { CreateProperties } from '../properties/source/CreateIO';
import { TextIOSourceProperties } from '../properties/source/TextIO';
import { JdbcIOSourceProperties } from '../properties/source/JdbcIO';
import { KafkaIOSourceProperties } from '../properties/source/KafkaIO';
import { KinesisIOSourceProperties } from '../properties/source/KinesisIO';
import { ParquetIOSourceProperties } from '../properties/source/ParquetIO';
import { AvroIOSourceProperties } from '../properties/source/AvroIO';
import { MqttIOSourceProperties } from '../properties/source/MqttIO';
import { PulsarIOSourceProperties } from '../properties/source/PulsarIO';

import { PubSubSinkProperties } from '../properties/sink/PubSubIO';
import { BigQuerySinkProperties } from '../properties/sink/BigQueryIO';
import { TextIOSinkProperties } from '../properties/sink/TextIO';
import { JdbcIOSinkProperties } from '../properties/sink/JdbcIO';
import { KafkaIOSinkProperties } from '../properties/sink/KafkaIO';
import { KinesisIOSinkProperties } from '../properties/sink/KinesisIO';
import { ParquetIOSinkProperties } from '../properties/sink/ParquetIO';
import { AvroIOSinkProperties } from '../properties/sink/AvroIO';
import { MqttIOSinkProperties } from '../properties/sink/MqttIO';
import { PulsarIOSinkProperties } from '../properties/sink/PulsarIO';

import { ParseMessageProperties } from '../properties/step/ParseMessage';
import { FilterProperties } from '../properties/step/Filter';
import { AggregationProperties } from '../properties/step/Aggregations';
import { WatermarkProperties } from '../properties/step/Watermark';
import { GroupByAggProperties } from '../properties/step/GroupByAgg';

/**
 * Resolves the concrete (eagerly imported) panel component a node dispatches to.
 * Registry entries are code-split lazy wrappers, so identity assertions against the
 * real component must go through `loadPropertyForm`.
 *
 * @param node Node-like object with immutable metadata.
 * @returns The concrete component, or undefined when unregistered.
 */
async function resolvePropertyForm(node: unknown) {
  const entry = getPropertyFormComponent(node);
  return entry ? loadPropertyForm(entry) : undefined;
}

/**
 * Waits until the Suspense placeholder for the lazily-loaded panel has gone away
 * (tolerates the chunk already being cached, in which case it never appears).
 */
const waitForPanelToLoad = () =>
  waitFor(() => expect(screen.queryByTestId('property-form-loading')).not.toBeInTheDocument());

describe('PropertyForm', () => {
  // =========================================================================
  // Core Regression Tests: Immutable Metadata vs User-Editable Label
  // =========================================================================

  it('REGRESSION: renders BigQuery form when label contains a different connector name (e.g. "my PubSub-ish step")', async () => {
    const mockNode = {
      data: {
        label: 'my PubSub-ish step',
        type: 'SINK',
        spark_transform: 'BigQueryIO',
        properties: {}
      }
    };
    const mockOnChange = vi.fn();

    render(<PropertyForm node={mockNode} onChange={mockOnChange} />);

    // Must render BigQuerySinkProperties (with Table Name placeholder)
    expect(await screen.findByPlaceholderText('my_table')).toBeInTheDocument();
    // Must NOT render PubSub properties
    expect(screen.queryByPlaceholderText('projects/my-project/subscriptions/my-sub')).not.toBeInTheDocument();
  });

  it('REGRESSION: renders PubSub form when label contains a different connector name (e.g. "BigQuery Ingestion")', async () => {
    const mockNode = {
      data: {
        label: 'BigQuery Ingestion',
        type: 'SOURCE',
        spark_transform: 'PubSubIO',
        properties: {}
      }
    };
    const mockOnChange = vi.fn();

    render(<PropertyForm node={mockNode} onChange={mockOnChange} />);

    // Must render PubSubProperties
    expect(await screen.findByPlaceholderText('projects/my-project/subscriptions/my-sub')).toBeInTheDocument();
    // Must NOT render BigQuery properties
    expect(screen.queryByPlaceholderText('my_table')).not.toBeInTheDocument();
  });

  it('REGRESSION: renders Kafka form when node is renamed away from connector name to a custom label', async () => {
    const mockNode = {
      data: {
        label: 'Custom Ingest Queue #42',
        type: 'SOURCE',
        spark_transform: 'KafkaIO',
        properties: {
          bootstrapServers: 'localhost:9092',
          topics: 'orders'
        }
      }
    };
    const mockOnChange = vi.fn();

    render(<PropertyForm node={mockNode} onChange={mockOnChange} />);

    expect(await screen.findByLabelText(/Bootstrap Servers/i)).toHaveValue('localhost:9092');
    expect(screen.getByLabelText(/Topic\(s\)/i)).toHaveValue('orders');
  });

  // =========================================================================
  // Unregistered / Missing Metadata Fallback Tests (Fail-Fast)
  // =========================================================================

  it('renders explicit unregistered state when spark_transform is unknown', () => {
    const mockNode = {
      data: {
        label: 'Custom Operator',
        spark_transform: 'NonExistentTransform123',
        properties: {}
      }
    };
    const mockOnChange = vi.fn();

    render(<PropertyForm node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByText('No property form registered for type "NonExistentTransform123"')).toBeInTheDocument();
  });

  it('renders explicit unregistered state when metadata is completely missing even if label matches a connector', () => {
    const mockNode = {
      data: {
        label: 'PubSub Source',
        properties: {}
      }
    };
    const mockOnChange = vi.fn();

    render(<PropertyForm node={mockNode} onChange={mockOnChange} />);

    // Must fail fast and NOT dispatch on the label
    expect(screen.getByText('No property form registered for type "unknown"')).toBeInTheDocument();
    expect(screen.queryByPlaceholderText('projects/my-project/topics/my-topic')).not.toBeInTheDocument();
  });

  it('renders explicit unregistered state showing node type when spark_transform is missing but type is present', () => {
    const mockNode = {
      data: {
        label: 'Unknown Sink Step',
        type: 'SINK',
        properties: {}
      }
    };
    const mockOnChange = vi.fn();

    render(<PropertyForm node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByText('No property form registered for type "SINK"')).toBeInTheDocument();
  });

  it('renders explicit unregistered state when node is null or undefined', () => {
    const mockOnChange = vi.fn();

    render(<PropertyForm node={null} onChange={mockOnChange} />);

    expect(screen.getByText('No property form registered for type "unknown"')).toBeInTheDocument();
  });

  // =========================================================================
  // Metadata Resolution & Lookup Branches
  // =========================================================================

  it('resolves componentType metadata as alternative to spark_transform', async () => {
    const mockNode = {
      data: {
        label: 'Custom Component',
        type: 'STEP',
        componentType: 'Filter',
        properties: {}
      }
    };
    const Component = await resolvePropertyForm(mockNode);
    expect(Component).toBeDefined();

    const mockOnChange = vi.fn();
    render(<PropertyForm node={mockNode} onChange={mockOnChange} />);
    expect(await screen.findByText('Code Generation Instructions')).toBeInTheDocument();
  });

  describe('getPropertyFormComponent resolution branches', () => {
    it('branch 1: resolves via type-qualified key (${type}:${transform})', async () => {
      const node = { data: { type: 'SOURCE', spark_transform: 'PubSubIO', properties: {} } };
      expect(await resolvePropertyForm(node)).toBe(PubSubProperties);
    });

    it('branch 2: prevents cross-direction fallback when opposite type is registered', () => {
      const rateSinkNode = { data: { type: 'SINK', spark_transform: 'RateSource', properties: {} } };
      expect(getPropertyFormComponent(rateSinkNode)).toBeUndefined();
      const arrowSourceNode = { data: { type: 'SOURCE', spark_transform: 'ArrowFlightIO', properties: {} } };
      expect(getPropertyFormComponent(arrowSourceNode)).toBeUndefined();
    });
  });

  // =========================================================================
  // Dedicated Identity Resolution Table (replaces ~60 individual panel render tests)
  // =========================================================================

  const ROUTING_CASES = [
    // Source IOs
    { type: 'SOURCE', spark_transform: 'PubSubIO', ExpectedComponent: PubSubProperties, name: 'PubSubProperties' },
    { type: 'SOURCE', spark_transform: 'BigQueryIO', ExpectedComponent: BigQuerySourceProperties, name: 'BigQuerySourceProperties' },
    { type: 'SOURCE', spark_transform: 'Create', ExpectedComponent: CreateProperties, name: 'CreateProperties' },
    { type: 'SOURCE', spark_transform: 'TextIO', ExpectedComponent: TextIOSourceProperties, name: 'TextIOSourceProperties' },
    { type: 'SOURCE', spark_transform: 'JdbcIO', ExpectedComponent: JdbcIOSourceProperties, name: 'JdbcIOSourceProperties' },
    { type: 'SOURCE', spark_transform: 'KafkaIO', ExpectedComponent: KafkaIOSourceProperties, name: 'KafkaIOSourceProperties' },
    { type: 'SOURCE', spark_transform: 'KinesisIO', ExpectedComponent: KinesisIOSourceProperties, name: 'KinesisIOSourceProperties' },
    { type: 'SOURCE', spark_transform: 'ParquetIO', ExpectedComponent: ParquetIOSourceProperties, name: 'ParquetIOSourceProperties' },
    { type: 'SOURCE', spark_transform: 'AvroIO', ExpectedComponent: AvroIOSourceProperties, name: 'AvroIOSourceProperties' },
    { type: 'SOURCE', spark_transform: 'MqttIO', ExpectedComponent: MqttIOSourceProperties, name: 'MqttIOSourceProperties' },
    { type: 'SOURCE', spark_transform: 'PulsarIO', ExpectedComponent: PulsarIOSourceProperties, name: 'PulsarIOSourceProperties' },

    // Sink IOs
    { type: 'SINK', spark_transform: 'PubSubIO', ExpectedComponent: PubSubSinkProperties, name: 'PubSubSinkProperties' },
    { type: 'SINK', spark_transform: 'BigQueryIO', ExpectedComponent: BigQuerySinkProperties, name: 'BigQuerySinkProperties' },
    { type: 'SINK', spark_transform: 'TextIO', ExpectedComponent: TextIOSinkProperties, name: 'TextIOSinkProperties' },
    { type: 'SINK', spark_transform: 'JdbcIO', ExpectedComponent: JdbcIOSinkProperties, name: 'JdbcIOSinkProperties' },
    { type: 'SINK', spark_transform: 'KafkaIO', ExpectedComponent: KafkaIOSinkProperties, name: 'KafkaIOSinkProperties' },
    { type: 'SINK', spark_transform: 'KinesisIO', ExpectedComponent: KinesisIOSinkProperties, name: 'KinesisIOSinkProperties' },
    { type: 'SINK', spark_transform: 'ParquetIO', ExpectedComponent: ParquetIOSinkProperties, name: 'ParquetIOSinkProperties' },
    { type: 'SINK', spark_transform: 'AvroIO', ExpectedComponent: AvroIOSinkProperties, name: 'AvroIOSinkProperties' },
    { type: 'SINK', spark_transform: 'MqttIO', ExpectedComponent: MqttIOSinkProperties, name: 'MqttIOSinkProperties' },
    { type: 'SINK', spark_transform: 'PulsarIO', ExpectedComponent: PulsarIOSinkProperties, name: 'PulsarIOSinkProperties' },

    // Step Transforms & Aliases
    { type: 'STEP', spark_transform: 'ParseMessage', ExpectedComponent: ParseMessageProperties, name: 'ParseMessageProperties' },
    { type: 'STEP', spark_transform: 'Filter', ExpectedComponent: FilterProperties, name: 'FilterProperties' },
    { type: 'STEP', spark_transform: 'Combine', ExpectedComponent: AggregationProperties, name: 'AggregationProperties' },
    { type: 'STEP', spark_transform: 'Watermark', ExpectedComponent: WatermarkProperties, name: 'WatermarkProperties' },
    { type: 'STEP', spark_transform: 'GroupByAgg', ExpectedComponent: GroupByAggProperties, name: 'GroupByAggProperties' },
  ];

  it.each(ROUTING_CASES)(
    'resolves $type:$spark_transform to $name',
    async ({ type, spark_transform, ExpectedComponent }) => {
      expect(
        await resolvePropertyForm({
          data: { type, spark_transform, properties: {} },
        })
      ).toBe(ExpectedComponent);
    }
  );

  it('does NOT register or resolve unsupported native directions (SOURCE:SplunkIO, SINK:GoogleAdsIO, SOURCE:DatadogIO)', () => {
    expect(PROPERTY_FORM_REGISTRY['SOURCE:SplunkIO']).toBeUndefined();
    expect(PROPERTY_FORM_REGISTRY['SINK:GoogleAdsIO']).toBeUndefined();
    expect(PROPERTY_FORM_REGISTRY['SOURCE:DatadogIO']).toBeUndefined();

    expect(getPropertyFormComponent({ data: { type: 'SOURCE', spark_transform: 'SplunkIO' } })).toBeUndefined();
    expect(getPropertyFormComponent({ data: { type: 'SINK', spark_transform: 'GoogleAdsIO' } })).toBeUndefined();
    expect(getPropertyFormComponent({ data: { type: 'SOURCE', spark_transform: 'DatadogIO' } })).toBeUndefined();

    const { rerender } = render(
      <PropertyForm node={{ data: { type: 'SOURCE', spark_transform: 'SplunkIO' } }} onChange={vi.fn()} />
    );
    expect(screen.getByText('No property form registered for type "SplunkIO"')).toBeInTheDocument();

    rerender(<PropertyForm node={{ data: { type: 'SINK', spark_transform: 'GoogleAdsIO' } }} onChange={vi.fn()} />);
    expect(screen.getByText('No property form registered for type "GoogleAdsIO"')).toBeInTheDocument();

    rerender(<PropertyForm node={{ data: { type: 'SOURCE', spark_transform: 'DatadogIO' } }} onChange={vi.fn()} />);
    expect(screen.getByText('No property form registered for type "DatadogIO"')).toBeInTheDocument();
  });
});

describe('Pipeline YAML Acceptance Tests', () => {
  it('parses a streaming Kafka -> ParseMessage -> Watermark -> GroupByAgg -> Iceberg pipeline and resolves every node without unregistered warnings', async () => {
    const sparkStreamingYaml = `
nodes:
  - id: src-1
    data:
      label: Kafka Stream
      type: SOURCE
      spark_transform: KafkaIO
      properties:
        bootstrapServers: localhost:9092
        topics: events
  - id: step-1
    data:
      label: Parse JSON
      type: STEP
      spark_transform: ParseMessage
      properties:
        messageType: JSON
  - id: step-2
    data:
      label: Event Watermark
      type: STEP
      spark_transform: Watermark
      properties:
        eventTimeColumn: event_timestamp
        delayThreshold: 10 minutes
  - id: step-3
    data:
      label: Windowed Metrics
      type: STEP
      spark_transform: GroupByAgg
      properties:
        groupByColumns:
          - region
        windowType: Tumbling
        timeColumn: event_timestamp
        windowDuration: 5 minutes
        aggregations:
          - column: amount
            fn: sum
            alias: total_amount
  - id: sink-1
    data:
      label: Iceberg Table
      type: SINK
      spark_transform: IcebergIO
      properties: {}
`;
    const parsed = YAML.parse(sparkStreamingYaml);
    expect(Array.isArray(parsed.nodes)).toBe(true);

    const migratedNodes = migrateLegacyNodes(parsed.nodes);
    expect(migratedNodes.length).toBe(5);

    for (const node of migratedNodes) {
      const mockOnChange = vi.fn();
      const { container, unmount } = render(<PropertyForm node={node} onChange={mockOnChange} />);
      await waitForPanelToLoad();

      expect(screen.queryByText(/No property form registered for type/i)).not.toBeInTheDocument();
      await waitFor(() => expect(container.querySelectorAll('input, select, textarea').length).toBeGreaterThan(0));
      unmount();
    }
  });

  it('loads any bundled .spark sample files when present and resolves every node without unregistered warnings', async () => {
    const samplesDir = path.resolve(__dirname, '../../../resources/samples');
    if (!fs.existsSync(samplesDir)) {
      return;
    }
    const sampleFiles = fs.readdirSync(samplesDir).filter((f) => f.endsWith('.spark'));
    for (const sampleFile of sampleFiles) {
      const content = fs.readFileSync(path.join(samplesDir, sampleFile), 'utf8');
      const parsed = YAML.parse(content);
      const migratedNodes = migrateLegacyNodes(parsed.nodes || []);
      for (const node of migratedNodes) {
        const { container, unmount } = render(<PropertyForm node={node} onChange={vi.fn()} />);
        await waitForPanelToLoad();
        expect(screen.queryByText(/No property form registered for type/i)).not.toBeInTheDocument();
        await waitFor(() => expect(container.querySelectorAll('input, select, textarea').length).toBeGreaterThan(0));
        unmount();
      }
    }
  });
});

// =========================================================================
// Messaging & Streaming Connector Dispatch (IO15, IO20, IO49, IO62, IO63)
// =========================================================================

describe('PropertyForm dispatch for messaging & streaming connectors', () => {
  const ALL_CONNECTORS = [
    { transform: 'AmqpIO', label: 'AMQP', task: 'IO15' },
    { transform: 'RabbitMqIO', label: 'RabbitMQ', task: 'IO20' },
    { transform: 'FlinkStreamingImpulseSource', label: 'Flink Streaming Impulse', task: 'IO49' },
    { transform: 'PulsarIO', label: 'Pulsar', task: 'IO62' },
    { transform: 'SolaceIO', label: 'Solace', task: 'IO63' },
  ];

  it.each(ALL_CONNECTORS)('never resolves $transform from a renamed user label alone ($task)', ({ transform }) => {
    // Regression guard for M14: dispatch must ignore the user-editable label.
    const renamedNode = { data: { label: transform, type: 'SOURCE', properties: {} } };
    expect(getPropertyFormComponent(renamedNode)).toBeUndefined();
  });
});

// =========================================================================
// Node Documentation & Docs Link Across Property Panels
// =========================================================================

describe('Node Documentation & Docs Link Across Property Panels', () => {
  it('renders a short description and a single "Docs ↗" link that dispatches openExternal for Source, Step, and Sink panels', async () => {
    const postMessage = vi.fn();
    (window as unknown as { vscode?: { postMessage: typeof postMessage } }).vscode = { postMessage };

    try {
      const sampleNodes = [
        { data: { label: 'PubSub Source', type: 'SOURCE', spark_transform: 'PubSubIO', properties: {} } },
        { data: { label: 'Watermark Step', type: 'STEP', spark_transform: 'Watermark', properties: {} } },
        { data: { label: 'GroupBy & Aggregate Step', type: 'STEP', spark_transform: 'GroupByAgg', properties: {} } },
        { data: { label: 'Combine Step', type: 'STEP', spark_transform: 'Combine', properties: {} } },
        { data: { label: 'Filter Step', type: 'STEP', spark_transform: 'Filter', properties: {} } },
        { data: { label: 'BigQuery Sink', type: 'SINK', spark_transform: 'BigQueryIO', properties: {} } },
        { data: { label: 'Iceberg Sink', type: 'SINK', spark_transform: 'IcebergIO', properties: {} } },
      ];

      for (const { data } of sampleNodes) {
        postMessage.mockClear();
        const doc = getNodeDocumentation({ data });
        expect(doc).toBeDefined();

        const { unmount } = render(<PropertyForm node={{ data }} onChange={vi.fn()} />);
        await waitForPanelToLoad();

        expect(screen.getByText(doc!.description)).toBeInTheDocument();
        const docsButtons = screen.getAllByText('Docs ↗');
        expect(docsButtons).toHaveLength(1);

        fireEvent.click(docsButtons[0]);
        expect(postMessage).toHaveBeenCalledWith({
          command: 'openExternal',
          url: doc!.docsUrl,
        });

        unmount();
      }
    } finally {
      delete (window as unknown as { vscode?: unknown }).vscode;
    }
  });
});

// =========================================================================
// Shared Streaming Sink Overrides, Step Cache Output, and Source Read Mode (A9 / A11)
// =========================================================================

describe('PropertyForm shared execution & streaming controls (A9 / A11)', () => {
  it('renders StreamingSinkSettingsField automatically for streaming SINK nodes and omits it for batch SINK nodes', async () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <PropertyForm
        node={{
          id: 'bq-sink-1',
          data: {
            label: 'BigQuery Sink',
            type: 'SINK',
            spark_transform: 'BigQueryIO',
            executionMode: 'batch',
            properties: {},
          },
        }}
        onChange={onChange}
      />
    );
    await waitForPanelToLoad();
    expect(screen.queryByText('Streaming Sink Settings')).not.toBeInTheDocument();

    rerender(
      <PropertyForm
        node={{
          id: 'bq-sink-1',
          data: {
            label: 'BigQuery Sink',
            type: 'SINK',
            spark_transform: 'BigQueryIO',
            executionMode: 'streaming',
            properties: {},
          },
        }}
        onChange={onChange}
      />
    );
    await waitForPanelToLoad();

    expect(screen.getByText('Streaming Sink Settings')).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText(/Output Mode/i), { target: { value: 'complete' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ outputMode: 'complete' }));
  });

  it('renders manual Cache Output override on batch STEP nodes and hides it on streaming STEP nodes', async () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <PropertyForm
        node={{
          id: 'filter-1',
          data: {
            label: 'Filter',
            type: 'STEP',
            spark_transform: 'Filter',
            executionMode: 'batch',
            properties: {},
          },
        }}
        onChange={onChange}
      />
    );
    await waitForPanelToLoad();

    const cacheSelect = screen.getByLabelText(/Cache Output/i);
    expect(cacheSelect).toHaveValue('auto');
    fireEvent.change(cacheSelect, { target: { value: 'true' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ cacheOutput: true }));

    rerender(
      <PropertyForm
        node={{
          id: 'filter-1',
          data: {
            label: 'Filter',
            type: 'STEP',
            spark_transform: 'Filter',
            executionMode: 'streaming',
            properties: {},
          },
        }}
        onChange={onChange}
      />
    );
    await waitForPanelToLoad();
    expect(screen.queryByLabelText(/Cache Output/i)).not.toBeInTheDocument();
  });

  it('renders Read Mode selector on dual-mode SOURCE nodes and hides it on batch-only SOURCE nodes', async () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <PropertyForm
        node={{
          id: 'src-bq',
          data: {
            label: 'BigQuery Source',
            type: 'SOURCE',
            spark_transform: 'BigQueryIO',
            supportedReadModes: ['batch'],
            properties: {},
          },
        }}
        onChange={onChange}
      />
    );
    await waitForPanelToLoad();
    expect(screen.queryByLabelText(/Read Mode/i)).not.toBeInTheDocument();

    rerender(
      <PropertyForm
        node={{
          id: 'src-kafka',
          data: {
            label: 'Kafka Source',
            type: 'SOURCE',
            spark_transform: 'KafkaIO',
            supportedReadModes: ['batch', 'streaming'],
            defaultReadMode: 'streaming',
            properties: {},
          },
        }}
        onChange={onChange}
      />
    );
    await waitForPanelToLoad();

    const readModeSelect = screen.getAllByLabelText(/Read Mode/i)[0];
    expect(readModeSelect).toHaveValue('streaming');
    fireEvent.change(readModeSelect, { target: { value: 'batch' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ readMode: 'batch' }));
  });
});

