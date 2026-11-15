/**
 * @fileoverview Unit tests for nodeKinds runtime predicate functions.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import {
  extractLabel,
  isFileIoSourceItem,
  isParseMessageItem,
  isPubSubNode,
  isPubSubSourceNode,
  isFhirIoNode,
  isDicomIoNode,
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

  describe('isFileIoSourceItem', () => {
    it('returns true when type is SOURCE and label is FileIO', () => {
      expect(isFileIoSourceItem({ type: 'SOURCE', label: 'FileIO' })).toBe(true);
    });

    it('returns true when type is SOURCE and sparkTransform is FileIO', () => {
      expect(
        isFileIoSourceItem({ type: 'SOURCE', label: 'Custom File Source', sparkTransform: 'FileIO' })
      ).toBe(true);
    });

    it('returns false when type is not SOURCE', () => {
      expect(isFileIoSourceItem({ type: 'STEP', label: 'FileIO' })).toBe(false);
      expect(isFileIoSourceItem({ type: 'SINK', label: 'FileIO', sparkTransform: 'FileIO' })).toBe(false);
    });

    it('returns false when neither label nor sparkTransform is FileIO', () => {
      expect(isFileIoSourceItem({ type: 'SOURCE', label: 'PubSub', sparkTransform: 'PubSubIO' })).toBe(false);
      expect(isFileIoSourceItem(null)).toBe(false);
      expect(isFileIoSourceItem(undefined)).toBe(false);
    });
  });

  describe('isParseMessageItem', () => {
    it('returns true for exact label Parse Message', () => {
      expect(isParseMessageItem({ label: 'Parse Message' })).toBe(true);
    });

    it('returns false for case variations or different labels', () => {
      expect(isParseMessageItem({ label: 'parse message' })).toBe(false);
      expect(isParseMessageItem({ label: 'PARSE MESSAGE' })).toBe(false);
      expect(isParseMessageItem({ label: 'Parse Message Step' })).toBe(false);
      expect(isParseMessageItem({ label: 'Filter' })).toBe(false);
      expect(isParseMessageItem(null)).toBe(false);
      expect(isParseMessageItem(undefined)).toBe(false);
    });
  });

  describe('isPubSubNode', () => {
    it('matches stable discriminator spark_transform PubSubIO regardless of label', () => {
      expect(isPubSubNode({ data: { spark_transform: 'PubSubIO', label: 'Renamed Ingest Stream' } })).toBe(true);
      expect(isPubSubNode({ spark_transform: 'PubSubIO', label: 'Custom Topic' })).toBe(true);
    });

    it('matches fallback label containing PubSub when spark_transform is absent', () => {
      expect(isPubSubNode({ data: { label: 'PubSub' } })).toBe(true);
      expect(isPubSubNode({ data: { label: 'Google PubSub Source' } })).toBe(true);
      expect(isPubSubNode({ label: 'PubSub Topic' })).toBe(true);
    });

    it('does NOT match lower-case or unrelated labels without spark_transform', () => {
      expect(isPubSubNode({ data: { label: 'pubsub' } })).toBe(false);
      expect(isPubSubNode({ data: { label: 'PUBSUB' } })).toBe(false);
      expect(isPubSubNode({ data: { label: 'Kafka' } })).toBe(false);
      expect(isPubSubNode(null)).toBe(false);
      expect(isPubSubNode(undefined)).toBe(false);
    });
  });

  describe('isPubSubSourceNode', () => {
    it('returns true when type is SOURCE and spark_transform is PubSubIO even with renamed label', () => {
      expect(
        isPubSubSourceNode({
          data: {
            type: 'SOURCE',
            spark_transform: 'PubSubIO',
            label: 'Telemetry Queue',
          },
        })
      ).toBe(true);
    });

    it('returns true when type is SOURCE and label includes PubSub', () => {
      expect(
        isPubSubSourceNode({
          data: {
            type: 'SOURCE',
            label: 'PubSub Reader',
          },
        })
      ).toBe(true);
    });

    it('returns false when type is not SOURCE', () => {
      expect(
        isPubSubSourceNode({
          data: {
            type: 'STEP',
            spark_transform: 'PubSubIO',
            label: 'PubSub Step',
          },
        })
      ).toBe(false);
    });

    it('returns false when node is SOURCE but not PubSub', () => {
      expect(
        isPubSubSourceNode({
          data: {
            type: 'SOURCE',
            spark_transform: 'BigQueryIO',
            label: 'BigQuery Source',
          },
        })
      ).toBe(false);
      expect(isPubSubSourceNode(null)).toBe(false);
      expect(isPubSubSourceNode(undefined)).toBe(false);
    });
  });

  describe('isFhirIoNode', () => {
    it('matches stable discriminator spark_transform FhirIO regardless of label', () => {
      expect(isFhirIoNode({ data: { spark_transform: 'FhirIO', label: 'Clinical Store' } })).toBe(true);
      expect(isFhirIoNode({ spark_transform: 'FhirIO', label: 'Healthcare API' })).toBe(true);
    });

    it('matches fallback label containing FhirIO when spark_transform is absent', () => {
      expect(isFhirIoNode({ data: { label: 'FhirIO' } })).toBe(true);
      expect(isFhirIoNode({ data: { label: 'GCP FhirIO Connector' } })).toBe(true);
      expect(isFhirIoNode({ label: 'FhirIO Source' })).toBe(true);
    });

    it('does NOT match lower-case or unrelated labels without spark_transform', () => {
      expect(isFhirIoNode({ data: { label: 'fhirio' } })).toBe(false);
      expect(isFhirIoNode({ data: { label: 'FHIRIO' } })).toBe(false);
      expect(isFhirIoNode({ data: { label: 'DicomIO' } })).toBe(false);
      expect(isFhirIoNode(null)).toBe(false);
      expect(isFhirIoNode(undefined)).toBe(false);
    });
  });

  describe('isDicomIoNode', () => {
    it('matches stable discriminator spark_transform DicomIO regardless of label', () => {
      expect(isDicomIoNode({ data: { spark_transform: 'DicomIO', label: 'Medical Images' } })).toBe(true);
      expect(isDicomIoNode({ spark_transform: 'DicomIO', label: 'PACS Store' })).toBe(true);
    });

    it('matches fallback label containing DicomIO when spark_transform is absent', () => {
      expect(isDicomIoNode({ data: { label: 'DicomIO' } })).toBe(true);
      expect(isDicomIoNode({ data: { label: 'GCP DicomIO Connector' } })).toBe(true);
      expect(isDicomIoNode({ label: 'DicomIO Source' })).toBe(true);
    });

    it('does NOT match lower-case or unrelated labels without spark_transform', () => {
      expect(isDicomIoNode({ data: { label: 'dicomio' } })).toBe(false);
      expect(isDicomIoNode({ data: { label: 'DICOMIO' } })).toBe(false);
      expect(isDicomIoNode({ data: { label: 'FhirIO' } })).toBe(false);
      expect(isDicomIoNode(null)).toBe(false);
      expect(isDicomIoNode(undefined)).toBe(false);
    });
  });
});
