/**
 * Unit Tests for NodesCard Component
 * 
 * Author: Mike Nimer <mikenimer@google.com>
 * 
 * Purpose:
 * Validates the vertical palette navigation, step/sink enablement based on node count,
 * search filtering, category rendering, theming classes, and VS Code state persistence.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { NodesCard, DEFAULT_SAMPLES } from './NodesCard';

describe('NodesCard', () => {
  const mockOnDragStart = vi.fn();
  const mockOnAddNode = vi.fn();

  it('renders vertical menu tabs and disables STEPS and SINKS when nodesCount is 0', () => {
    render(
      <NodesCard
        activeTab="SOURCES"
        setActiveTab={vi.fn()}
        nodesCount={0}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
      />
    );

    expect(screen.getByText('SOURCES')).toBeInTheDocument();
    const sourcesTab = screen.getByLabelText('Sources Tab');
    const stepsTab = screen.getByLabelText('Steps Tab');
    const sinksTab = screen.getByLabelText('Sinks Tab');

    expect(sourcesTab).toBeInTheDocument();
    expect(stepsTab).toBeDisabled();
    expect(sinksTab).toBeDisabled();
  });

  it('enables STEPS and SINKS when nodesCount > 0', () => {
    render(
      <NodesCard
        activeTab="SOURCES"
        setActiveTab={vi.fn()}
        nodesCount={1}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
      />
    );

    const stepsTab = screen.getByLabelText('Steps Tab');
    const sinksTab = screen.getByLabelText('Sinks Tab');

    expect(stepsTab).not.toBeDisabled();
    expect(sinksTab).not.toBeDisabled();
  });

  it('calls setActiveTab when clicking enabled tab', () => {
    const mockSetActiveTab = vi.fn();
    render(
      <NodesCard
        activeTab="SOURCES"
        setActiveTab={mockSetActiveTab}
        nodesCount={1}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
      />
    );

    fireEvent.click(screen.getByLabelText('Steps Tab'));
    expect(mockSetActiveTab).toHaveBeenCalledWith('STEPS');
  });

  it('toggles tab to null when clicking already active tab', () => {
    const mockSetActiveTab = vi.fn();
    render(
      <NodesCard
        activeTab="SOURCES"
        setActiveTab={mockSetActiveTab}
        nodesCount={1}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
      />
    );

    fireEvent.click(screen.getByLabelText('Sources Tab'));
    expect(mockSetActiveTab).toHaveBeenCalledWith(null);
  });

  it('toggles panel open/closed when close button is clicked', () => {
    const mockOnToggleOpen = vi.fn();
    const mockSetActiveTab = vi.fn();
    render(
      <NodesCard
        activeTab="SOURCES"
        setActiveTab={mockSetActiveTab}
        nodesCount={0}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
        isOpen={true}
        onToggleOpen={mockOnToggleOpen}
      />
    );

    const closeButton = screen.getByRole('button', { name: /Close panel/i });
    fireEvent.click(closeButton);

    expect(mockOnToggleOpen).toHaveBeenCalledWith(false);
    expect(mockSetActiveTab).toHaveBeenCalledWith(null);
  });

  it('does not render panel when activeTab is null', () => {
    render(
      <NodesCard
        activeTab={null}
        setActiveTab={vi.fn()}
        nodesCount={1}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
      />
    );

    expect(screen.queryByRole('button', { name: /Close panel/i })).not.toBeInTheDocument();
    expect(screen.queryByText('Streaming Sources')).not.toBeInTheDocument();
  });

  it('filters components using search input', () => {
    render(
      <NodesCard
        activeTab="SOURCES"
        setActiveTab={vi.fn()}
        nodesCount={0}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
      />
    );

    const searchInput = screen.getByPlaceholderText('Search sources...');
    fireEvent.change(searchInput, { target: { value: 'Kafka' } });

    expect(screen.getByText('Kafka')).toBeInTheDocument();
    expect(screen.queryByText('BigQuery')).not.toBeInTheDocument();
  });

  it('renders sources items when activeTab is SOURCES', () => {
    render(
      <NodesCard
        activeTab="SOURCES"
        setActiveTab={vi.fn()}
        nodesCount={0}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
      />
    );

    expect(screen.getByText(/SOURCES \(38\)/i)).toBeInTheDocument();
    expect(screen.getByText('Streaming Sources')).toBeInTheDocument();
    expect(screen.getByText('Lakehouse Sources')).toBeInTheDocument();
    expect(screen.getByText('File Format Sources')).toBeInTheDocument();
    expect(screen.getByText('Warehouse & Database Sources')).toBeInTheDocument();
    expect(screen.getByText('NoSQL & Graph Sources')).toBeInTheDocument();
    expect(screen.getByText('Synthetic & API Sources')).toBeInTheDocument();
    expect(screen.getByText('Kafka')).toBeInTheDocument();
    expect(screen.getByText('Pub/Sub')).toBeInTheDocument();
    expect(screen.getByText('Event Hubs')).toBeInTheDocument();
    expect(screen.getByText('MQTT')).toBeInTheDocument();
    expect(screen.getByText('Rate')).toBeInTheDocument();
    expect(screen.getByText('Socket')).toBeInTheDocument();
    expect(screen.getByText('BigQuery')).toBeInTheDocument();
    expect(screen.getByText('JDBC Database')).toBeInTheDocument();
    expect(screen.getByLabelText('Add Parquet')).toBeInTheDocument();
    expect(screen.getByLabelText('Add Text')).toBeInTheDocument();
    expect(screen.getByText('Create DataFrame')).toBeInTheDocument();
  });

  it('renders STEPS categories and items', () => {
    render(
      <NodesCard
        activeTab="STEPS"
        setActiveTab={vi.fn()}
        nodesCount={1}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
      />
    );

    expect(screen.getByText(/STEPS & TRANSFORMS \(29\)/i)).toBeInTheDocument();
    expect(screen.getByText('Column & Row')).toBeInTheDocument();
    expect(screen.getByText('Joins & Sets')).toBeInTheDocument();
    expect(screen.getByText('Aggregations')).toBeInTheDocument();
    expect(screen.getByText('Streaming')).toBeInTheDocument();
    expect(screen.getByText('Custom & SQL')).toBeInTheDocument();
    expect(screen.getByText('Partitioning')).toBeInTheDocument();
    expect(screen.getByText('Spark ML & AI')).toBeInTheDocument();

    expect(screen.getByText('Select')).toBeInTheDocument();
    expect(screen.getByText('Filter')).toBeInTheDocument();
    expect(screen.getByText('Parse Payload')).toBeInTheDocument();
    expect(screen.getByText('Join')).toBeInTheDocument();
    expect(screen.getByText('Union By Name')).toBeInTheDocument();
    expect(screen.getByText('GroupBy & Aggregate')).toBeInTheDocument();
    expect(screen.getByText('Watermark')).toBeInTheDocument();
    expect(screen.getByText('Stateful Transform')).toBeInTheDocument();
    expect(screen.getByText('Spark SQL Query')).toBeInTheDocument();
    expect(screen.getByText('Repartition / Coalesce')).toBeInTheDocument();
    expect(screen.getByText('Spark ML & Vertex AI')).toBeInTheDocument();
    expect(screen.queryByText('ParDo')).not.toBeInTheDocument();
    expect(screen.queryByText('Co Group By Key')).not.toBeInTheDocument();
  });

  it('renders SINKS categories and items', () => {
    render(
      <NodesCard
        activeTab="SINKS"
        setActiveTab={vi.fn()}
        nodesCount={1}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
      />
    );

    expect(screen.getByText(/SINKS \(37\)/i)).toBeInTheDocument();
    expect(screen.getByText('Streaming Sinks')).toBeInTheDocument();
    expect(screen.getByText('Lakehouse Sinks')).toBeInTheDocument();
    expect(screen.getByText('File Format Sinks')).toBeInTheDocument();
    expect(screen.getByText('Warehouse & Database Sinks')).toBeInTheDocument();
    expect(screen.getByText('NoSQL & Graph Sinks')).toBeInTheDocument();
    expect(screen.getByText('Debug & Custom Sinks')).toBeInTheDocument();

    expect(screen.getByText('Kafka')).toBeInTheDocument();
    expect(screen.getByText('Pub/Sub')).toBeInTheDocument();
    expect(screen.getByText('Event Hubs')).toBeInTheDocument();
    expect(screen.getByText('MQTT')).toBeInTheDocument();
    expect(screen.getByText('BigQuery')).toBeInTheDocument();
    expect(screen.getByText('JDBC Database')).toBeInTheDocument();
    expect(screen.getByLabelText('Add Parquet')).toBeInTheDocument();
    expect(screen.getByLabelText('Add Text')).toBeInTheDocument();
    expect(screen.getByText('Console')).toBeInTheDocument();
    expect(screen.getByText('Memory')).toBeInTheDocument();
    expect(screen.getByText('ForeachBatch')).toBeInTheDocument();
  });

  it('applies green theme highlight to SOURCES tab when active', () => {
    render(
      <NodesCard
        activeTab="SOURCES"
        setActiveTab={vi.fn()}
        nodesCount={0}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        isOpen={true}
      />
    );

    const sourcesTab = screen.getByLabelText('Sources Tab');
    expect(sourcesTab).toHaveClass('border-emerald-500/90');
    expect(sourcesTab).toHaveClass('text-emerald-400');
  });

  it('applies blue theme highlight to STEPS tab when active', () => {
    render(
      <NodesCard
        activeTab="STEPS"
        setActiveTab={vi.fn()}
        nodesCount={1}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        isOpen={true}
      />
    );

    const stepsTab = screen.getByLabelText('Steps Tab');
    expect(stepsTab).toHaveClass('border-blue-500/90');
    expect(stepsTab).toHaveClass('text-blue-400');
  });

  it('applies red theme highlight to SINKS tab when active', () => {
    render(
      <NodesCard
        activeTab="SINKS"
        setActiveTab={vi.fn()}
        nodesCount={1}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        isOpen={true}
      />
    );

    const sinksTab = screen.getByLabelText('Sinks Tab');
    expect(sinksTab).toHaveClass('border-rose-500/90');
    expect(sinksTab).toHaveClass('text-rose-400');
  });

  it('renders SAMPLES tab as enabled even when nodesCount is 0', () => {
    render(
      <NodesCard
        activeTab="SOURCES"
        setActiveTab={vi.fn()}
        nodesCount={0}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
      />
    );

    const samplesTab = screen.getByLabelText('Samples Tab');
    expect(samplesTab).toBeInTheDocument();
    expect(samplesTab).not.toBeDisabled();
    expect(screen.getByText('SAMPLES')).toBeInTheDocument();
  });

  it('applies yellow/amber theme highlight to SAMPLES tab when active', () => {
    render(
      <NodesCard
        activeTab="SAMPLES"
        setActiveTab={vi.fn()}
        nodesCount={0}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        isOpen={true}
      />
    );

    const samplesTab = screen.getByLabelText('Samples Tab');
    expect(samplesTab).toHaveClass('border-amber-500/90');
    expect(samplesTab).toHaveClass('text-amber-400');
  });

  it('renders sample items when activeTab is SAMPLES and handles Info and New File buttons', () => {
    const mockOnOpenSample = vi.fn();
    const customSamples = [
      {
        id: 'sample-1',
        filename: 'custom-sample.spark',
        title: 'Custom Sample Title',
        description: 'Test custom sample description',
        badge: 'Java',
        readmeContent: '# Custom Sample Title\n\n## Overview\nDetailed README documentation for custom sample.',
        content: 'content: test'
      }
    ];

    render(
      <NodesCard
        activeTab="SAMPLES"
        setActiveTab={vi.fn()}
        nodesCount={0}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
        samples={customSamples}
        onOpenSample={mockOnOpenSample}
      />
    );

    expect(screen.getByText(/SAMPLES \(/i)).toBeInTheDocument();
    expect(screen.getByText('Custom Sample Title')).toBeInTheDocument();
    expect(screen.getByText('custom-sample.spark')).toBeInTheDocument();
    expect(screen.getByText('Test custom sample description')).toBeInTheDocument();
    expect(screen.queryByText('Java')).not.toBeInTheDocument();

    // Click Info button to open README modal
    fireEvent.click(screen.getByTestId('sample-info-btn-sample-1'));
    expect(screen.getByTestId('sample-info-modal')).toBeInTheDocument();
    expect(screen.getByText('Detailed README documentation for custom sample.')).toBeInTheDocument();
    fireEvent.click(screen.getByTestId('sample-info-modal-close'));
    expect(screen.queryByTestId('sample-info-modal')).not.toBeInTheDocument();

    // Click New File button to trigger save dialog / open sample
    fireEvent.click(screen.getByTestId('sample-new-file-btn-sample-1'));
    expect(mockOnOpenSample).toHaveBeenCalledWith(customSamples[0]);
  });

  it('filters sample items using search input in SAMPLES tab', () => {
    const customSamples = [
      {
        id: 'sample-1',
        filename: 'generate-mock.spark',
        title: 'Generate Mock Data',
        description: 'Generates test records',
        badge: 'Java',
        content: ''
      },
      {
        id: 'sample-2',
        filename: 'pubsub-to-bigquery.spark',
        title: 'PubSub to BigQuery',
        description: 'Streams data to BQ',
        badge: 'Python',
        content: ''
      }
    ];

    render(
      <NodesCard
        activeTab="SAMPLES"
        setActiveTab={vi.fn()}
        nodesCount={0}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        onAddNode={mockOnAddNode}
        samples={customSamples}
      />
    );

    const searchInput = screen.getByPlaceholderText('Search samples...');
    fireEvent.change(searchInput, { target: { value: 'BigQuery' } });

    expect(screen.getByText('PubSub to BigQuery')).toBeInTheDocument();
    expect(screen.queryByText('Generate Mock Data')).not.toBeInTheDocument();
  });

  it('renders menu container with matching top and bottom padding and zero gap to panel', () => {
    const { container } = render(
      <NodesCard
        activeTab="SOURCES"
        setActiveTab={vi.fn()}
        nodesCount={0}
        hasPubSubSource={false}
        onDragStart={mockOnDragStart}
        isOpen={true}
      />
    );

    const outerContainer = container.firstElementChild;
    expect(outerContainer).toHaveClass('left-3');
    expect(outerContainer).toHaveClass('top-3');
    expect(outerContainer).toHaveClass('bottom-3');
  });

  describe('VS Code State & Group Persistence', () => {
    let mockStorage: Record<string, string> = {};

    beforeEach(() => {
      mockStorage = {};
      const localStorageMock = {
        getItem: vi.fn((key: string) => mockStorage[key] || null),
        setItem: vi.fn((key: string, value: string) => {
          mockStorage[key] = value.toString();
        }),
        removeItem: vi.fn((key: string) => {
          delete mockStorage[key];
        }),
        clear: vi.fn(() => {
          mockStorage = {};
        }),
      };
      Object.defineProperty(window, 'localStorage', {
        value: localStorageMock,
        writable: true,
        configurable: true,
      });
      delete (window as unknown as { vscode?: unknown }).vscode;
    });

    it('persists group state changes to vscode.setState when available', () => {
      const mockSetState = vi.fn();
      const mockGetState = vi.fn().mockReturnValue({ customProp: 'hello' });
      (window as unknown as { vscode?: unknown }).vscode = {
        getState: mockGetState,
        setState: mockSetState,
      };

      const { container } = render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={0} hasPubSubSource={false} onDragStart={mockOnDragStart} />
      );

      const streamingDetails = container.querySelector('details');
      expect(streamingDetails).toBeInTheDocument();

      // Trigger toggle event
      if (streamingDetails) {
        streamingDetails.open = false;
        fireEvent(streamingDetails, new Event('toggle'));
      }

      expect(mockSetState).toHaveBeenCalledWith(
        expect.objectContaining({
          customProp: 'hello',
          nodePaletteGroups: expect.objectContaining({
            sources_streaming_sources: false,
          }),
        })
      );
    });

    it('persists group state changes to localStorage', () => {
      const { container } = render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={0} hasPubSubSource={false} onDragStart={mockOnDragStart} />
      );

      const streamingDetails = container.querySelector('details');
      if (streamingDetails) {
        streamingDetails.open = false;
        fireEvent(streamingDetails, new Event('toggle'));
      }

      const stored = localStorage.getItem('beam_node_groups_state');
      expect(stored).toBeTruthy();
      const parsed = JSON.parse(stored!);
      expect(parsed.sources_streaming_sources).toBe(false);
    });

    it('restores group state from vscode.getState when opening', () => {
      (window as unknown as { vscode?: unknown }).vscode = {
        getState: () => ({
          nodePaletteGroups: {
            sources_streaming_sources: false,
            sources_lakehouse_sources: true,
          },
        }),
        setState: vi.fn(),
      };

      const { container } = render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={0} hasPubSubSource={false} onDragStart={mockOnDragStart} />
      );

      const detailsList = container.querySelectorAll('details');
      // sources_streaming_sources is first details
      expect(detailsList[0].open).toBe(false);
      // sources_lakehouse_sources is second details
      expect(detailsList[1].open).toBe(true);
    });

    it('renders Kinesis in Streaming Sources and Streaming Sinks tabs', () => {
      const { rerender } = render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={1} hasPubSubSource={false} onDragStart={mockOnDragStart} onAddNode={mockOnAddNode} />
      );

      expect(screen.getByText('Kinesis')).toBeInTheDocument();
      expect(screen.getByText('Streaming Sources')).toBeInTheDocument();

      rerender(
        <NodesCard activeTab="SINKS" setActiveTab={vi.fn()} nodesCount={1} hasPubSubSource={false} onDragStart={mockOnDragStart} onAddNode={mockOnAddNode} />
      );

      expect(screen.getByText('Kinesis')).toBeInTheDocument();
      expect(screen.getByText('Streaming Sinks')).toBeInTheDocument();
    });

    it('renders Avro, Parquet, and Iceberg in SOURCES and SINKS under File Format and Lakehouse groups', () => {
      const { rerender } = render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={0} hasPubSubSource={false} onDragStart={mockOnDragStart} />
      );

      expect(screen.getAllByText('Avro').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Parquet').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Iceberg').length).toBeGreaterThan(0);
      expect(screen.getByText('File Format Sources')).toBeInTheDocument();
      expect(screen.getByText('Lakehouse Sources')).toBeInTheDocument();

      rerender(
        <NodesCard activeTab="SINKS" setActiveTab={vi.fn()} nodesCount={1} hasPubSubSource={false} onDragStart={mockOnDragStart} />
      );

      expect(screen.getAllByText('Avro').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Parquet').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Iceberg').length).toBeGreaterThan(0);
      expect(screen.queryByText('TikaIO')).not.toBeInTheDocument();
      expect(screen.getByText('File Format Sinks')).toBeInTheDocument();
      expect(screen.getByText('Lakehouse Sinks')).toBeInTheDocument();
    });

    it('collapses other groups when a user opens/expands a group in a panel', () => {
      const { container } = render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={0} hasPubSubSource={false} onDragStart={mockOnDragStart} />
      );

      const detailsList = container.querySelectorAll('details');
      expect(detailsList.length).toBeGreaterThan(1);

      // By default, the first group (Streaming Sources) is open, second (Database Sources) is closed
      expect(detailsList[0].open).toBe(true);
      expect(detailsList[1].open).toBe(false);

      // Expand the second group (Database Sources)
      detailsList[1].open = true;
      fireEvent(detailsList[1], new Event('toggle'));

      // The second group should now be open, and the first group must be collapsed
      expect(detailsList[1].open).toBe(true);
      expect(detailsList[0].open).toBe(false);
    });

    it('sizes tab panels to fill the padded visible diagram height with a scrollable container inside', () => {
      const { container, rerender } = render(
        <NodesCard
          activeTab="SOURCES"
          setActiveTab={vi.fn()}
          nodesCount={1}
          hasPubSubSource={false}
          onDragStart={mockOnDragStart}
          isOpen={true}
        />
      );

      const outerContainer = container.firstElementChild as HTMLElement;
      expect(outerContainer).toHaveClass('top-3');
      expect(outerContainer).toHaveClass('bottom-3');
      expect(outerContainer).toHaveClass('h-[calc(100%-1.5rem)]');
      expect(outerContainer).toHaveClass('max-h-[calc(100%-1.5rem)]');

      const verticalMenu = outerContainer.children[0] as HTMLElement;
      expect(verticalMenu).toHaveClass('max-h-full');

      const slideOutPanel = outerContainer.children[1] as HTMLElement;
      expect(slideOutPanel).toHaveClass('h-full');
      expect(slideOutPanel).toHaveClass('max-h-full');
      expect(slideOutPanel).toHaveClass('overflow-hidden');

      const scrollContainer = slideOutPanel.children[2] as HTMLElement;
      expect(scrollContainer).toHaveClass('flex-1');
      expect(scrollContainer).toHaveClass('min-h-0');
      expect(scrollContainer).toHaveClass('overflow-y-auto');

      rerender(
        <NodesCard
          activeTab="SAMPLES"
          setActiveTab={vi.fn()}
          nodesCount={1}
          hasPubSubSource={false}
          onDragStart={mockOnDragStart}
          isOpen={true}
        />
      );

      const samplesSlideOutPanel = outerContainer.children[1] as HTMLElement;
      expect(samplesSlideOutPanel).toHaveClass('h-full');
      expect(samplesSlideOutPanel).toHaveClass('max-h-full');

      const samplesScrollContainer = samplesSlideOutPanel.children[2] as HTMLElement;
      expect(samplesScrollContainer).toHaveClass('flex-1');
      expect(samplesScrollContainer).toHaveClass('min-h-0');
      expect(samplesScrollContainer).toHaveClass('overflow-y-auto');
    });

    it('sorts items in each category list alphabetically across SOURCES, STEPS, and SINKS', () => {
      const { container, rerender } = render(
        <NodesCard
          activeTab="SOURCES"
          setActiveTab={vi.fn()}
          nodesCount={1}
          hasPubSubSource={false}
          onDragStart={mockOnDragStart}
        />
      );

      const assertDetailsListsSorted = () => {
        const detailsList = container.querySelectorAll('details');
        expect(detailsList.length).toBeGreaterThan(0);
        detailsList.forEach((details) => {
          const itemNodes = details.querySelectorAll('[draggable="true"]');
          const labels = Array.from(itemNodes).map(
            (node) => node.getAttribute('aria-label')?.replace(/^Add /, '') || ''
          );
          const sortedLabels = [...labels].sort((a, b) =>
            a.localeCompare(b, undefined, { sensitivity: 'base' })
          );
          expect(labels).toEqual(sortedLabels);
        });
      };

      assertDetailsListsSorted();

      rerender(
        <NodesCard
          activeTab="STEPS"
          setActiveTab={vi.fn()}
          nodesCount={1}
          hasPubSubSource={false}
          onDragStart={mockOnDragStart}
        />
      );
      assertDetailsListsSorted();

      rerender(
        <NodesCard
          activeTab="SINKS"
          setActiveTab={vi.fn()}
          nodesCount={1}
          hasPubSubSource={false}
          onDragStart={mockOnDragStart}
        />
      );
      assertDetailsListsSorted();
    });

    it('sorts sample items alphabetically by title in the SAMPLES tab', () => {
      const unsortedSamples = [
        {
          id: 'sample-z',
          filename: 'z-pipeline.spark',
          title: 'Zebra Pipeline',
          description: 'Z description',
          badge: 'Python',
          content: ''
        },
        {
          id: 'sample-a',
          filename: 'a-pipeline.spark',
          title: 'Alpha Pipeline',
          description: 'A description',
          badge: 'Java',
          content: ''
        },
        {
          id: 'sample-m',
          filename: 'm-pipeline.spark',
          title: 'Metrics Pipeline',
          description: 'M description',
          badge: 'Python',
          content: ''
        }
      ];

      render(
        <NodesCard
          activeTab="SAMPLES"
          setActiveTab={vi.fn()}
          nodesCount={0}
          hasPubSubSource={false}
          onDragStart={mockOnDragStart}
          samples={unsortedSamples}
        />
      );

      const sampleButtons = screen.getAllByTitle(/Save ".*" as a new \.spark file and open in editor/i);
      const renderedTitles = sampleButtons.map((el) => el.getAttribute('title') || '');
      expect(renderedTitles[0]).toContain('Alpha Pipeline');
      expect(renderedTitles[1]).toContain('Metrics Pipeline');
      expect(renderedTitles[2]).toContain('Zebra Pipeline');
    });

    it('provides four Apache Spark 4.x PySpark fallback entries in DEFAULT_SAMPLES using only .spark filenames', () => {
      render(
        <NodesCard
          activeTab="SAMPLES"
          setActiveTab={vi.fn()}
          nodesCount={0}
          hasPubSubSource={false}
          onDragStart={mockOnDragStart}
          isOpen={true}
        />
      );

      expect(screen.getByText(/Samples \(4\)/i)).toBeInTheDocument();
      expect(screen.getByText('gcs-parquet-etl.spark')).toBeInTheDocument();
      expect(screen.getByText('kafka-streaming-to-iceberg.spark')).toBeInTheDocument();
      expect(screen.getByText('bigquery-cloudsql-broadcast-join.spark')).toBeInTheDocument();
      expect(screen.getByText('rate-stream-to-delta.spark')).toBeInTheDocument();
      expect(DEFAULT_SAMPLES.every((s) => s.filename.endsWith('.spark'))).toBe(true);
    });

    it('renders Spark 4.x Source categories and omits legacy Beam-only Source nodes', () => {
      const { rerender } = render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={1} hasPubSubSource={false} onDragStart={mockOnDragStart} onAddNode={mockOnAddNode} />
      );

      // Categories in SOURCES (Phase 5 SRC1-SRC38)
      expect(screen.getByText('Streaming Sources')).toBeInTheDocument();
      expect(screen.getByText('Lakehouse Sources')).toBeInTheDocument();
      expect(screen.getByText('File Format Sources')).toBeInTheDocument();
      expect(screen.getByText('Warehouse & Database Sources')).toBeInTheDocument();
      expect(screen.getByText('NoSQL & Graph Sources')).toBeInTheDocument();
      expect(screen.getByText('Synthetic & API Sources')).toBeInTheDocument();

      // Unsupported Source directions must NOT appear in SOURCES
      expect(screen.queryByText('SplunkIO')).not.toBeInTheDocument();
      expect(screen.queryByText('DatadogIO')).not.toBeInTheDocument();

      // Switch to SINKS tab
      rerender(
        <NodesCard activeTab="SINKS" setActiveTab={vi.fn()} nodesCount={1} hasPubSubSource={false} onDragStart={mockOnDragStart} onAddNode={mockOnAddNode} />
      );

      // Categories in SINKS (Phase 6 SNK1-SNK37)
      expect(screen.getByText('Streaming Sinks')).toBeInTheDocument();
      expect(screen.getByText('Lakehouse Sinks')).toBeInTheDocument();
      expect(screen.getByText('File Format Sinks')).toBeInTheDocument();
      expect(screen.getByText('Warehouse & Database Sinks')).toBeInTheDocument();
      expect(screen.getByText('NoSQL & Graph Sinks')).toBeInTheDocument();
      expect(screen.getByText('Debug & Custom Sinks')).toBeInTheDocument();

      // Legacy Beam-only sinks must NOT appear in SINKS palette
      expect(screen.queryByText('Arrow Flight')).not.toBeInTheDocument();
      expect(screen.queryByText('DatadogIO')).not.toBeInTheDocument();
      expect(screen.queryByText('SplunkIO')).not.toBeInTheDocument();
    });
  });

  // =======================================================================
  // Phase 5 Spark 4.x Source Connector Palette (SRC1-SRC38)
  // =======================================================================
  describe('Spark 4.x Source connector palette entries (SRC1-SRC38)', () => {
    const STREAMING_CONNECTORS = [
      { label: 'Kafka', sourceDescription: 'Consume streaming or batch records from Apache Kafka topics' },
      { label: 'Pulsar', sourceDescription: 'Consume streaming or batch messages from Apache Pulsar topics' },
      { label: 'Event Hubs', sourceDescription: 'Consume streaming or batch events from Azure Event Hubs' },
      { label: 'Rate', sourceDescription: 'Generate streaming benchmark rows at a configurable rate per second' }
    ];

    it.each(STREAMING_CONNECTORS)('renders $label in Streaming Sources', ({ label, sourceDescription }) => {
      render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={1} hasPubSubSource={false} onDragStart={mockOnDragStart} onAddNode={mockOnAddNode} />
      );

      expect(screen.getByText(label)).toBeInTheDocument();
      expect(screen.getByText(sourceDescription)).toBeInTheDocument();
      expect(screen.getByText('Streaming Sources')).toBeInTheDocument();
    });

    it('renders Delta Lake, Apache Hudi, Iceberg, and Hive Table under Lakehouse Sources', () => {
      render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={1} hasPubSubSource={false} onDragStart={mockOnDragStart} onAddNode={mockOnAddNode} />
      );

      const lakehouseGroup = screen.getByText('Lakehouse Sources').closest('details') as HTMLElement;
      expect(lakehouseGroup).not.toBeNull();
      for (const label of ['Iceberg', 'Delta Lake', 'Apache Hudi', 'Hive Table']) {
        expect(lakehouseGroup.querySelector(`[aria-label="Add ${label}"]`)).not.toBeNull();
      }
    });

    it('keeps the new connector nodes discoverable through palette search', () => {
      render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={1} hasPubSubSource={false} onDragStart={mockOnDragStart} onAddNode={mockOnAddNode} />
      );

      const searchInput = screen.getByPlaceholderText(/search/i);
      fireEvent.change(searchInput, { target: { value: 'pulsar' } });

      expect(screen.getByText('Pulsar')).toBeInTheDocument();
      expect(screen.queryByText('RabbitMQ')).not.toBeInTheDocument();
    });

    it('groups Warehouse & Database Sources and NoSQL & Graph Sources cleanly in SOURCES', () => {
      render(
        <NodesCard activeTab="SOURCES" setActiveTab={vi.fn()} nodesCount={1} hasPubSubSource={false} onDragStart={mockOnDragStart} onAddNode={mockOnAddNode} />
      );

      const dbSourcesGroup = screen.getByText('Warehouse & Database Sources').closest('details') as HTMLElement;
      expect(dbSourcesGroup).not.toBeNull();
      for (const label of [
        'BigQuery',
        'Cloud Spanner',
        'Cloud Bigtable',
        'Cloud SQL',
        'AlloyDB',
        'JDBC Database',
        'Snowflake',
        'Amazon Redshift',
        'ClickHouse',
        'SingleStore',
      ]) {
        expect(dbSourcesGroup.querySelector(`[aria-label="Add ${label}"]`)).not.toBeNull();
      }

      const nosqlSourcesGroup = screen.getByText('NoSQL & Graph Sources').closest('details') as HTMLElement;
      expect(nosqlSourcesGroup).not.toBeNull();
      for (const label of ['MongoDB', 'Cloud Firestore', 'Apache Cassandra', 'Redis', 'Elasticsearch', 'Neo4j']) {
        expect(nosqlSourcesGroup.querySelector(`[aria-label="Add ${label}"]`)).not.toBeNull();
      }
    });

    it('groups Lakehouse, Warehouse & Database, NoSQL & Graph, and Debug & Custom Sinks cleanly in SINKS (SNK1-SNK37)', () => {
      render(
        <NodesCard activeTab="SINKS" setActiveTab={vi.fn()} nodesCount={1} hasPubSubSource={false} onDragStart={mockOnDragStart} onAddNode={mockOnAddNode} />
      );

      const lakehouseSinksGroup = screen.getByText('Lakehouse Sinks').closest('details') as HTMLElement;
      expect(lakehouseSinksGroup).not.toBeNull();
      for (const label of ['Iceberg', 'Delta Lake', 'Apache Hudi', 'Hive Table']) {
        expect(lakehouseSinksGroup.querySelector(`[aria-label="Add ${label}"]`)).not.toBeNull();
      }

      const dbSinksGroup = screen.getByText('Warehouse & Database Sinks').closest('details') as HTMLElement;
      expect(dbSinksGroup).not.toBeNull();
      for (const label of [
        'BigQuery',
        'Cloud Spanner',
        'Cloud Bigtable',
        'Cloud SQL',
        'AlloyDB',
        'JDBC Database',
        'Snowflake',
        'Amazon Redshift',
        'ClickHouse',
        'SingleStore',
      ]) {
        expect(dbSinksGroup.querySelector(`[aria-label="Add ${label}"]`)).not.toBeNull();
      }

      const nosqlSinksGroup = screen.getByText('NoSQL & Graph Sinks').closest('details') as HTMLElement;
      expect(nosqlSinksGroup).not.toBeNull();
      for (const label of ['MongoDB', 'Cloud Firestore', 'Apache Cassandra', 'Redis', 'Elasticsearch', 'Neo4j']) {
        expect(nosqlSinksGroup.querySelector(`[aria-label="Add ${label}"]`)).not.toBeNull();
      }

      const debugSinksGroup = screen.getByText('Debug & Custom Sinks').closest('details') as HTMLElement;
      expect(debugSinksGroup).not.toBeNull();
      for (const label of ['Console', 'Memory', 'ForeachBatch']) {
        expect(debugSinksGroup.querySelector(`[aria-label="Add ${label}"]`)).not.toBeNull();
      }
    });
  });
});



