/**
 * @fileoverview Unit tests for the dedicated property form component registry.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, expect, it } from 'vitest';
import { PROPERTY_FORM_REGISTRY, loadPropertyForm } from './propertyFormRegistry';
import { NODE_DOCUMENTATION_REGISTRY } from './documentation';
import { SOURCE_NODES } from './nodes/sources';
import { SINK_NODES } from './nodes/sinks';
import { STEP_NODES } from './nodes/steps';
import { SPARK_NODE_CAPABILITIES } from '../../../src/shared/sparkNodeCapabilities';

describe('PROPERTY_FORM_REGISTRY', () => {
  it('has dedicated panel entries matching the documented node catalog count and exact keys', () => {
    const registryKeys = Object.keys(PROPERTY_FORM_REGISTRY);
    const documentedKeys = Object.keys(NODE_DOCUMENTATION_REGISTRY);
    expect(registryKeys.length).toBe(documentedKeys.length);
    expect(registryKeys.length).toBeGreaterThan(0);
    expect(new Set(registryKeys)).toEqual(new Set(documentedKeys));
  });

  it('registers all 40 Spark 4.x SOURCE_NODES under both SOURCE:<transform> and direct <transform> keys', () => {
    expect(SOURCE_NODES).toHaveLength(40);
    for (const node of SOURCE_NODES) {
      const qualifiedKey = `SOURCE:${node.sparkTransform}`;
      expect(PROPERTY_FORM_REGISTRY[qualifiedKey], `Missing form for ${qualifiedKey}`).toBeDefined();
      expect(PROPERTY_FORM_REGISTRY[node.sparkTransform], `Missing direct form for ${node.sparkTransform}`).toBeDefined();
      expect(PROPERTY_FORM_REGISTRY[qualifiedKey]).toBe(PROPERTY_FORM_REGISTRY[node.sparkTransform]);
      expect(NODE_DOCUMENTATION_REGISTRY[qualifiedKey], `Missing docs for ${qualifiedKey}`).toBeDefined();
      expect(NODE_DOCUMENTATION_REGISTRY[node.sparkTransform], `Missing direct docs for ${node.sparkTransform}`).toBeDefined();
    }
  });

  it('registers all 38 Spark 4.x SINK_NODES under both SINK:<transform> and direct <transform> keys', () => {
    expect(SINK_NODES).toHaveLength(38);
    for (const node of SINK_NODES) {
      const qualifiedKey = `SINK:${node.sparkTransform}`;
      expect(PROPERTY_FORM_REGISTRY[qualifiedKey], `Missing form for ${qualifiedKey}`).toBeDefined();
      expect(PROPERTY_FORM_REGISTRY[node.sparkTransform], `Missing direct form for ${node.sparkTransform}`).toBeDefined();
      expect(PROPERTY_FORM_REGISTRY[qualifiedKey]).toBe(PROPERTY_FORM_REGISTRY[node.sparkTransform]);
      expect(NODE_DOCUMENTATION_REGISTRY[qualifiedKey], `Missing docs for ${qualifiedKey}`).toBeDefined();
      expect(NODE_DOCUMENTATION_REGISTRY[node.sparkTransform], `Missing direct docs for ${node.sparkTransform}`).toBeDefined();
    }
  });

  it('registers all 33 Spark 4.x STEP_NODES under both STEP:<transform> and direct <transform> keys with capability parity', () => {
    expect(STEP_NODES).toHaveLength(33);
    for (const node of STEP_NODES) {
      const qualifiedKey = `STEP:${node.sparkTransform}`;
      expect(PROPERTY_FORM_REGISTRY[qualifiedKey], `Missing form for ${qualifiedKey}`).toBeDefined();
      expect(PROPERTY_FORM_REGISTRY[node.sparkTransform], `Missing direct form for ${node.sparkTransform}`).toBeDefined();
      expect(PROPERTY_FORM_REGISTRY[qualifiedKey]).toBe(PROPERTY_FORM_REGISTRY[node.sparkTransform]);
      expect(NODE_DOCUMENTATION_REGISTRY[qualifiedKey], `Missing docs for ${qualifiedKey}`).toBeDefined();
      expect(NODE_DOCUMENTATION_REGISTRY[node.sparkTransform], `Missing direct docs for ${node.sparkTransform}`).toBeDefined();

      const capability = SPARK_NODE_CAPABILITIES[node.sparkTransform];
      expect(capability, `Missing SPARK_NODE_CAPABILITIES entry for ${node.sparkTransform}`).toBeDefined();
      expect(node.inputArity).toBe(capability.inputArity);
      expect(node.errorOutput).toBe(capability.errorOutput);
    }
    expect(PROPERTY_FORM_REGISTRY['STEP:SparkMlInference']).toBe(PROPERTY_FORM_REGISTRY['SparkMlAndVertexAi']);
    expect(PROPERTY_FORM_REGISTRY['SparkMlInference']).toBe(PROPERTY_FORM_REGISTRY['SparkMlAndVertexAi']);
    expect(PROPERTY_FORM_REGISTRY['STEP:VertexAiInference']).toBe(PROPERTY_FORM_REGISTRY['SparkMlAndVertexAi']);
    expect(PROPERTY_FORM_REGISTRY['VertexAiInference']).toBe(PROPERTY_FORM_REGISTRY['SparkMlAndVertexAi']);
  });

  it('contains valid component functions or objects for every registered key', () => {
    for (const [key, Component] of Object.entries(PROPERTY_FORM_REGISTRY)) {
      expect(Component, `Entry for key "${key}" must be defined`).toBeDefined();
      const isFunction = typeof Component === 'function';
      const isObjectComponent = typeof Component === 'object' && Component !== null;
      expect(
        isFunction || isObjectComponent,
        `Entry for key "${key}" must be a function or component object, got ${typeof Component}`
      ).toBe(true);
    }
  });

  it('registers every panel as a code-split lazy component (P8 bundle splitting)', () => {
    // Eagerly importing even one panel would pull it back into the entry bundle.
    for (const [key, Component] of Object.entries(PROPERTY_FORM_REGISTRY)) {
      expect(typeof Component, `Entry for key "${key}" must be a React.lazy wrapper`).toBe('object');
    }
  });

  it('resolves every lazy entry to a concrete component (guards dynamic import paths and named exports)', async () => {
    const unique = new Set(Object.values(PROPERTY_FORM_REGISTRY));
    const resolved = await Promise.all(
      [...unique].map(async (entry) => {
        const Component = await loadPropertyForm(entry);
        return { entry, Component };
      })
    );
    for (const { Component } of resolved) {
      expect(typeof Component).toBe('function');
    }
    // Distinct lazy wrappers must map to distinct components; aliases share a wrapper.
    expect(new Set(resolved.map((r) => r.Component)).size).toBe(unique.size);
  }, 60_000);

  it('memoises the underlying import so repeated loads return the same component', async () => {
    const entry = PROPERTY_FORM_REGISTRY['SOURCE:PubSubIO'];
    const [first, second] = await Promise.all([loadPropertyForm(entry), loadPropertyForm(entry)]);
    expect(first).toBe(second);
    expect(await loadPropertyForm(entry)).toBe(first);
  });

  it('passes eager (non-lazy) components through loadPropertyForm unchanged', async () => {
    const Eager = () => null;
    expect(await loadPropertyForm(Eager)).toBe(Eager);
  });
});

/** Expected node counts per palette type. */
const EXPECTED_COUNTS = { SOURCE: 40, SINK: 38, STEP: 33 } as const;
const EXPECTED_TOTAL = EXPECTED_COUNTS.SOURCE + EXPECTED_COUNTS.SINK + EXPECTED_COUNTS.STEP;

/** Catalog node lists keyed by palette type. */
const CATALOG_BY_TYPE = { SOURCE: SOURCE_NODES, SINK: SINK_NODES, STEP: STEP_NODES } as const;

/** Palette types in display order. */
const NODE_TYPES = ['SOURCE', 'SINK', 'STEP'] as const;

describe('PROPERTY_FORM_REGISTRY catalog coverage (T4)', () => {
  it('derives exact per-type and total node counts from the catalog', () => {
    for (const type of NODE_TYPES) {
      expect(CATALOG_BY_TYPE[type], `${type} node count`).toHaveLength(EXPECTED_COUNTS[type]);
    }
    const total = SOURCE_NODES.length + SINK_NODES.length + STEP_NODES.length;
    expect(total).toBe(EXPECTED_TOTAL);
  });

  it('declares the matching type, a non-empty sparkTransform, and a unique transform for every catalog node', () => {
    const seen = new Map<string, string>();
    for (const type of NODE_TYPES) {
      for (const node of CATALOG_BY_TYPE[type]) {
        expect(node.type, `${node.label} is listed under ${type}`).toBe(type);
        expect(node.sparkTransform.trim(), `${node.label} has a sparkTransform`).not.toBe('');
        expect(node.sparkTransform).toBe(node.sparkTransform.trim());
        // A direct key must map to exactly one catalog node, so transforms never repeat across types.
        expect(seen.get(node.sparkTransform), `Duplicate sparkTransform ${node.sparkTransform}`).toBeUndefined();
        seen.set(node.sparkTransform, type);
      }
    }
    expect(seen.size).toBe(EXPECTED_TOTAL);
  });

  it('has exactly one qualified canonical registry key per catalog node and type', () => {
    for (const type of NODE_TYPES) {
      const transforms = new Set(CATALOG_BY_TYPE[type].map((node) => node.sparkTransform));
      const canonicalKeys = Object.keys(PROPERTY_FORM_REGISTRY).filter(
        (key) => key.startsWith(`${type}:`) && transforms.has(key.slice(type.length + 1))
      );
      expect(canonicalKeys, `${type} canonical keys`).toHaveLength(EXPECTED_COUNTS[type]);
    }
  });

  it('never registers a catalog transform under the qualified key of another type', () => {
    const misfiled: string[] = [];
    for (const type of NODE_TYPES) {
      for (const otherType of NODE_TYPES.filter((t) => t !== type)) {
        for (const node of CATALOG_BY_TYPE[type]) {
          if (PROPERTY_FORM_REGISTRY[`${otherType}:${node.sparkTransform}`] !== undefined) {
            misfiled.push(`${otherType}:${node.sparkTransform}`);
          }
        }
      }
    }
    expect(misfiled).toEqual([]);
  });

  it('gives every catalog node a dedicated panel that no other catalog node shares', () => {
    const owners = new Map<unknown, string[]>();
    for (const type of NODE_TYPES) {
      for (const node of CATALOG_BY_TYPE[type]) {
        const form = PROPERTY_FORM_REGISTRY[`${type}:${node.sparkTransform}`];
        owners.set(form, [...(owners.get(form) ?? []), `${type}:${node.sparkTransform}`]);
      }
    }
    const shared = [...owners.values()].filter((keys) => keys.length > 1);
    expect(shared).toEqual([]);
    expect(owners.size).toBe(EXPECTED_TOTAL);
  });

  it('returns undefined for unknown, empty, and prefix-only keys', () => {
    expect(PROPERTY_FORM_REGISTRY['SOURCE:DoesNotExist']).toBeUndefined();
    expect(PROPERTY_FORM_REGISTRY['DoesNotExist']).toBeUndefined();
    expect(PROPERTY_FORM_REGISTRY['']).toBeUndefined();
    for (const type of NODE_TYPES) {
      expect(PROPERTY_FORM_REGISTRY[`${type}:`]).toBeUndefined();
    }
  });
});
