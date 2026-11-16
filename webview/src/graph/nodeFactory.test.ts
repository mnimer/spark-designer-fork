/**
 * @fileoverview Unit tests for nodeFactory module.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, afterEach } from 'vitest';
import { createNodeId, createNodesForPaletteItem, type PaletteItem } from './nodeFactory';
import type { PipelineNode } from '../hooks/usePipelineGraph';

describe('nodeFactory', () => {
  describe('createNodeId', () => {
    it('generates 1000 unique node IDs', () => {
      const ids = new Set<string>();
      for (let i = 0; i < 1000; i++) {
        const id = createNodeId();
        expect(ids.has(id)).toBe(false);
        ids.add(id);
      }
      expect(ids.size).toBe(1000);
    });

    it('preserves prefix correctly', () => {
      const srcId = createNodeId('source_');
      expect(srcId.startsWith('source_')).toBe(true);
      expect(srcId.length).toBeGreaterThan('source_'.length);

      const plainId = createNodeId();
      expect(plainId).not.toContain('source_');
    });

    describe('fallback when crypto.randomUUID is not available', () => {
      const originalCrypto = globalThis.crypto;

      afterEach(() => {
        Object.defineProperty(globalThis, 'crypto', {
          value: originalCrypto,
          configurable: true,
          writable: true,
        });
      });

      it('falls back to counter + Math.random when crypto.randomUUID is absent', () => {
        Object.defineProperty(globalThis, 'crypto', {
          value: {},
          configurable: true,
          writable: true,
        });

        const id1 = createNodeId('test_');
        const id2 = createNodeId('test_');

        expect(id1.startsWith('test_')).toBe(true);
        expect(id2.startsWith('test_')).toBe(true);
        expect(id1).not.toBe(id2);
      });
    });
  });

  describe('createNodesForPaletteItem', () => {
    it('creates a single Spark format source node without legacy multi-node expansion', () => {
      const item: PaletteItem = {
        label: 'CSV Source',
        type: 'SOURCE',
        iconName: 'FileText',
        sparkTransform: 'CsvSource',
      };
      const pos = { x: 100, y: 150 };

      const result = createNodesForPaletteItem(item, pos);

      expect(result.nodes).toHaveLength(1);
      expect(result.edges).toHaveLength(0);
      expect(result.nodes[0].type).toBe('pipeline');
      expect(result.nodes[0].position).toEqual(pos);
      expect(result.nodes[0].data).toEqual({
        label: 'CSV Source',
        type: 'SOURCE',
        iconName: 'FileText',
        spark_transform: 'CsvSource',
      });
    });

    it('preserves custom nodeType when passed', () => {
      const item: PaletteItem = {
        label: 'Parquet Sink',
        type: 'SINK',
        iconName: 'Database',
        sparkTransform: 'ParquetSink',
        nodeType: 'customPipelineType',
      };
      const result = createNodesForPaletteItem(item, { x: 50, y: 50 });

      expect(result.nodes).toHaveLength(1);
      expect(result.nodes[0].type).toBe('customPipelineType');
    });

    it('omits spark_transform when item does not provide sparkTransform', () => {
      const item: PaletteItem = {
        label: 'Custom Step',
        type: 'STEP',
        iconName: 'Cpu',
      };
      const result = createNodesForPaletteItem(item, { x: 0, y: 0 });

      expect(result.nodes).toHaveLength(1);
      expect(result.nodes[0].data.spark_transform).toBeUndefined();
    });

    describe('auto-edge wiring for STEP items', () => {
      it('auto-connects ParsePayload step to an existing PubSubSource node', () => {
        const pubsubNode: PipelineNode = {
          id: 'pubsub-123',
          type: 'pipeline',
          position: { x: 0, y: 0 },
          data: {
            label: 'Telemetry Stream',
            type: 'SOURCE',
            iconName: 'Radio',
            spark_transform: 'PubSubSource',
          },
        };

        const item: PaletteItem = {
          label: 'Parse Payload',
          type: 'STEP',
          iconName: 'FileCode',
          sparkTransform: 'ParsePayload',
        };

        const result = createNodesForPaletteItem(item, { x: 200, y: 0 }, [pubsubNode]);

        expect(result.nodes).toHaveLength(1);
        expect(result.edges).toHaveLength(1);
        expect(result.edges[0]).toEqual({
          id: `e-pubsub-123-${result.nodes[0].id}`,
          source: 'pubsub-123',
          target: result.nodes[0].id,
        });
      });

      it('auto-connects first STEP to existing SOURCE when no other STEP exists', () => {
        const sourceNode: PipelineNode = {
          id: 'src-1',
          type: 'pipeline',
          position: { x: 0, y: 0 },
          data: {
            label: 'Generic Source',
            type: 'SOURCE',
            iconName: 'Radio',
          },
        };

        const item: PaletteItem = {
          label: 'Filter',
          type: 'STEP',
          iconName: 'Filter',
          sparkTransform: 'Filter',
        };

        const result = createNodesForPaletteItem(item, { x: 200, y: 0 }, [sourceNode]);

        expect(result.nodes).toHaveLength(1);
        expect(result.edges).toHaveLength(1);
        expect(result.edges[0]).toEqual({
          id: `e-src-1-${result.nodes[0].id}`,
          source: 'src-1',
          target: result.nodes[0].id,
        });
      });

      it('auto-connects Join step to existing SOURCE on the left targetHandle (A6)', () => {
        const sourceNode: PipelineNode = {
          id: 'src-orders',
          type: 'pipeline',
          position: { x: 0, y: 0 },
          data: {
            label: 'Orders',
            type: 'SOURCE',
            iconName: 'Database',
            spark_transform: 'BigQuerySource',
          },
        };

        const item: PaletteItem = {
          label: 'Join',
          type: 'STEP',
          iconName: 'GitMerge',
          sparkTransform: 'Join',
        };

        const result = createNodesForPaletteItem(item, { x: 240, y: 0 }, [sourceNode]);

        expect(result.edges).toHaveLength(1);
        expect(result.edges[0]).toEqual({
          id: `e-src-orders-${result.nodes[0].id}`,
          source: 'src-orders',
          target: result.nodes[0].id,
          targetHandle: 'left',
        });
      });

      it('does NOT auto-connect STEP if a STEP already exists in the graph', () => {
        const sourceNode: PipelineNode = {
          id: 'src-1',
          type: 'pipeline',
          position: { x: 0, y: 0 },
          data: {
            label: 'Generic Source',
            type: 'SOURCE',
            iconName: 'Radio',
          },
        };
        const existingStepNode: PipelineNode = {
          id: 'step-1',
          type: 'pipeline',
          position: { x: 200, y: 0 },
          data: {
            label: 'First Step',
            type: 'STEP',
            iconName: 'Code',
          },
        };

        const item: PaletteItem = {
          label: 'Second Step',
          type: 'STEP',
          iconName: 'Code',
        };

        const result = createNodesForPaletteItem(
          item,
          { x: 400, y: 0 },
          [sourceNode, existingStepNode]
        );

        expect(result.nodes).toHaveLength(1);
        expect(result.edges).toHaveLength(0);
      });
    });
  });
});
