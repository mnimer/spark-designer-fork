/**
 * @fileoverview Compatibility re-export shim for the Spark execution planner.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Re-exports the canonical pure-TypeScript planner from the shared extension layer
 * so Webview hooks and components consume identical types and deterministic logic.
 */

export * from '../../../src/shared/executionPlanner';
