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

  it('registers all 38 Phase 5 Spark 4.x SOURCE_NODES under both SOURCE:<transform> and direct <transform> keys', () => {
    expect(SOURCE_NODES).toHaveLength(38);
    for (const node of SOURCE_NODES) {
      const qualifiedKey = `SOURCE:${node.sparkTransform}`;
      expect(PROPERTY_FORM_REGISTRY[qualifiedKey], `Missing form for ${qualifiedKey}`).toBeDefined();
      expect(PROPERTY_FORM_REGISTRY[node.sparkTransform], `Missing direct form for ${node.sparkTransform}`).toBeDefined();
      expect(PROPERTY_FORM_REGISTRY[qualifiedKey]).toBe(PROPERTY_FORM_REGISTRY[node.sparkTransform]);
      expect(NODE_DOCUMENTATION_REGISTRY[qualifiedKey], `Missing docs for ${qualifiedKey}`).toBeDefined();
      expect(NODE_DOCUMENTATION_REGISTRY[node.sparkTransform], `Missing direct docs for ${node.sparkTransform}`).toBeDefined();
    }
  });

  it('registers all 37 Phase 6 Spark 4.x SINK_NODES under both SINK:<transform> and direct <transform> keys', () => {
    expect(SINK_NODES).toHaveLength(37);
    for (const node of SINK_NODES) {
      const qualifiedKey = `SINK:${node.sparkTransform}`;
      expect(PROPERTY_FORM_REGISTRY[qualifiedKey], `Missing form for ${qualifiedKey}`).toBeDefined();
      expect(PROPERTY_FORM_REGISTRY[node.sparkTransform], `Missing direct form for ${node.sparkTransform}`).toBeDefined();
      expect(PROPERTY_FORM_REGISTRY[qualifiedKey]).toBe(PROPERTY_FORM_REGISTRY[node.sparkTransform]);
      expect(NODE_DOCUMENTATION_REGISTRY[qualifiedKey], `Missing docs for ${qualifiedKey}`).toBeDefined();
      expect(NODE_DOCUMENTATION_REGISTRY[node.sparkTransform], `Missing direct docs for ${node.sparkTransform}`).toBeDefined();
    }
  });

  it('registers all 29 Phase 7 Spark 4.x STEP_NODES under both STEP:<transform> and direct <transform> keys with capability parity', () => {
    expect(STEP_NODES).toHaveLength(29);
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
