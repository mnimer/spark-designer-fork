/**
 * @fileoverview Compatibility re-export shim for the Spark Declarative Pipelines planner.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Re-exports the canonical pure-TypeScript SDP planner from the shared extension layer
 * so Webview hooks and components consume identical types and deterministic logic.
 */

export * from '../../../src/shared/sdpPlanner';
export type {
    SdpBlockingIssue,
    SdpNodeCompatibility,
    SdpPlannedDataset,
    SdpExecutionPlan,
    SdpPlannerOptions,
} from '../../../src/shared/sdpPlanner';
