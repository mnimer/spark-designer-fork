/**
 * @fileoverview Compatibility re-export shim for Spark DAG rules and capability helpers.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

export * from '../../../src/shared/sparkDagRules';
export type {
  ConditionalSplitHandle,
  InputArity,
  JoinExecutionMode,
  NodeCapabilityFlags,
  ReadMode,
  StreamingPropagationResult,
  StreamingSinkMode,
} from '../../../src/shared/sparkDagRules';
