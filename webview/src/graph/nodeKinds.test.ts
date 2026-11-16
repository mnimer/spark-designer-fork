/**
 * @fileoverview Unit tests for nodeKinds runtime predicate functions.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import {
  extractLabel,
  isParseMessageItem,
  isPubSubNode,
  isPubSubSourceNode,
} from './nodeKinds';

describe('nodeKinds runtime predicates', () => {
  describe('extractLabel', () => {
    it('returns raw string when string is passed', () => {
      expect(extractLabel('SampleLabel')).toBe('SampleLabel');
    });

    it('extracts label from node data', () => {
      expect(extractLabel({ data: { label: 'NodeLabel' } })).toBe('NodeLabel');
    });

    it('extracts label from data object directly', () => {
      expect(extractLabel({ label: 'DataLabel' })).toBe('DataLabel');
    });

    it('returns undefined for missing or invalid inputs', () => {
      expect(extractLabel(null)).toBeUndefined();
      expect(extractLabel(undefined)).toBeUndefined();
      expect(extractLabel({})).toBeUndefined();
      expect(extractLabel({ data: {} })).toBeUndefined();
    });
  });

  describe('isParseMessageItem', () => {
    it('returns true for ParsePayload and Parse Message items', () => {
      expect(isParseMessageItem({ label: 'Parse Payload', sparkTransform: 'ParsePayload' })).toBe(true);
      expect(isParseMessageItem({ label: 'Custom Parser', sparkTransform: 'ParsePayload' })).toBe(true);
      expect(isParseMessageItem({ label: 'Parse Message' })).toBe(true);
      expect(isParseMessageItem({ sparkTransform: 'ParseMessage' })).toBe(true);
    });

    it('returns false for case variations or unrelated items', () => {
      expect(isParseMessageItem({ label: 'parse message' })).toBe(false);
      expect(isParseMessageItem({ label: 'Filter', sparkTransform: 'Filter' })).toBe(false);
      expect(isParseMessageItem(null)).toBe(false);
      expect(isParseMessageItem(undefined)).toBe(false);
    });
  });

  describe('isPubSubNode', () => {
    it('matches Spark PubSubSource, PubSubSink, and legacy PubSubIO regardless of label', () => {
      expect(isPubSubNode({ data: { spark_transform: 'PubSubSource', label: 'Telemetry Stream' } })).toBe(true);
      expect(isPubSubNode({ data: { spark_transform: 'PubSubSink', label: 'Events Out' } })).toBe(true);
      expect(isPubSubNode({ data: { spark_transform: 'PubSubIO', label: 'Renamed Ingest Stream' } })).toBe(true);
    });

    it('matches fallback label containing PubSub or Pub/Sub when spark_transform is absent', () => {
      expect(isPubSubNode({ data: { label: 'PubSub' } })).toBe(true);
      expect(isPubSubNode({ data: { label: 'Google Cloud Pub/Sub' } })).toBe(true);
      expect(isPubSubNode({ label: 'PubSub Topic' })).toBe(true);
    });

    it('does NOT match lower-case or unrelated labels without spark_transform', () => {
      expect(isPubSubNode({ data: { label: 'pubsub' } })).toBe(false);
      expect(isPubSubNode({ data: { label: 'Kafka' } })).toBe(false);
      expect(isPubSubNode(null)).toBe(false);
      expect(isPubSubNode(undefined)).toBe(false);
    });
  });

  describe('isPubSubSourceNode', () => {
    it('returns true when type is SOURCE and spark_transform is PubSubSource or PubSubIO', () => {
      expect(
        isPubSubSourceNode({
          data: {
            type: 'SOURCE',
            spark_transform: 'PubSubSource',
            label: 'Telemetry Queue',
          },
        })
      ).toBe(true);
      expect(
        isPubSubSourceNode({
          data: {
            type: 'SOURCE',
            spark_transform: 'PubSubIO',
            label: 'Legacy Stream',
          },
        })
      ).toBe(true);
    });

    it('returns false when type is not SOURCE or node is not PubSub', () => {
      expect(
        isPubSubSourceNode({
          data: {
            type: 'SINK',
            spark_transform: 'PubSubSink',
            label: 'PubSub Sink',
          },
        })
      ).toBe(false);
      expect(
        isPubSubSourceNode({
          data: {
            type: 'SOURCE',
            spark_transform: 'BigQuerySource',
            label: 'BigQuery Source',
          },
        })
      ).toBe(false);
      expect(isPubSubSourceNode(null)).toBe(false);
      expect(isPubSubSourceNode(undefined)).toBe(false);
    });
  });
});
