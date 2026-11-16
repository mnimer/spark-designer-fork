/**
 * @fileoverview Webview re-export shim for the Spark 4.x runtime version matrix.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

export * from '../../../src/spark/versionMatrix';
export type {
  SparkRuntimeTarget,
  SparkRunnerType,
  ServerlessTier,
  RuntimeProfile,
  ConnectorDeliveryMechanism,
  ConnectorResolution,
  PipelineRuntimeNodeInput,
  PipelineRuntimeArtifacts,
} from '../../../src/spark/versionMatrix';
