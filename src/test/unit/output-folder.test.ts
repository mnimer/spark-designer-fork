/**
 * @fileoverview Unit tests for output folder resolution and next available spark filename helpers.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import * as path from 'path';
import {
    resolveOutputFolder,
    nextAvailableSparkFilename
} from '../../editor/outputFolder';

describe('resolveOutputFolder', () => {
    const ws = path.resolve('/workspace/project');

    it('returns empty string when no spark path and no override are provided', () => {
        const result = resolveOutputFolder(undefined, undefined, ws);
        expect(result).toBe('');
    });

    it('derives default folder from spark file path', () => {
        const sparkPath = path.join('/a', 'b', 'my.spark');
        const expected = path.join('/a', 'b', 'my');
        const result = resolveOutputFolder(sparkPath, undefined, ws);
        expect(result).toBe(expected);
    });

    it('returns absolute override directly', () => {
        const sparkPath = path.join('/a', 'b', 'my.spark');
        const absoluteOverride = path.resolve('/custom/output/folder');
        const result = resolveOutputFolder(sparkPath, absoluteOverride, ws);
        expect(result).toBe(absoluteOverride);
    });

    it('resolves relative override with workspace folder', () => {
        const sparkPath = path.join('/a', 'b', 'my.spark');
        const relativeOverride = path.join('build', 'output');
        const expected = path.join(ws, relativeOverride);
        const result = resolveOutputFolder(sparkPath, relativeOverride, ws);
        expect(result).toBe(expected);
    });

    it('falls back to default for relative override when workspace folder is undefined', () => {
        const sparkPath = path.join('/a', 'b', 'my.spark');
        const relativeOverride = path.join('build', 'output');
        const expected = path.join('/a', 'b', 'my');
        const result = resolveOutputFolder(sparkPath, relativeOverride, undefined);
        expect(result).toBe(expected);
    });

    it('falls back to default when override contains only whitespace', () => {
        const sparkPath = path.join('/a', 'b', 'my.spark');
        const expected = path.join('/a', 'b', 'my');
        const result = resolveOutputFolder(sparkPath, '   ', ws);
        expect(result).toBe(expected);
    });
});

describe('nextAvailableSparkFilename', () => {
    const dir = path.resolve('/workspace/project');

    it('returns base.spark when name is free', () => {
        const exists = (_p: string) => false;
        const result = nextAvailableSparkFilename(dir, 'pipeline', exists);
        expect(result).toBe('pipeline.spark');
    });

    it('returns pipeline-1.spark when pipeline.spark is taken', () => {
        const takenPath = path.join(dir, 'pipeline.spark');
        const exists = (p: string) => p === takenPath;
        const result = nextAvailableSparkFilename(dir, 'pipeline', exists);
        expect(result).toBe('pipeline-1.spark');
    });

    it('returns pipeline-2.spark when pipeline.spark and pipeline-1.spark are taken', () => {
        const taken = new Set([
            path.join(dir, 'pipeline.spark'),
            path.join(dir, 'pipeline-1.spark'),
        ]);
        const exists = (p: string) => taken.has(p);
        const result = nextAvailableSparkFilename(dir, 'pipeline', exists);
        expect(result).toBe('pipeline-2.spark');
    });

    it('handles baseName given with .spark suffix', () => {
        const taken = new Set([
            path.join(dir, 'sample.spark'),
        ]);
        const exists = (p: string) => taken.has(p);
        const result = nextAvailableSparkFilename(dir, 'sample.spark', exists);
        expect(result).toBe('sample-1.spark');
    });
});
