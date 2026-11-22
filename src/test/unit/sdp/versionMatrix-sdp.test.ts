/**
 * @fileoverview Unit tests for Spark Declarative Pipelines (SDP) runtime profiles and capability flags.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies SDP runtime support flags across Serverless Spark 4.0 and 3.0, Dataproc Cluster 4.0 and 3.0,
 * Local Spark 4.2 Spark Connect, and Local Spark 4.0. Validates defensive array cloning, Managed Spark
 * classification, driver pip requirements, and local Iceberg Spark 4.2 missing-runtime flagging.
 */

import { describe, it, expect } from 'vitest';
import {
  RUNTIME_PROFILES,
  isManagedSparkRuntime,
  isRuntimeSdpCapable,
  resolveRuntimeProfile,
  resolveConnectorForRuntime,
  resolvePipelineRuntimeArtifacts,
} from '../../../spark/versionMatrix';

describe('SDP runtime version matrix profiles (SDP4)', () => {
  it('marks Serverless 4.0, Dataproc Cluster 4.0, and Local 4.2 Connect as SDP-capable', () => {
    expect(RUNTIME_PROFILES['serverless_4.0'].sdpSupported).toBe(true);
    expect(RUNTIME_PROFILES['serverless_4.0'].sparkVersion).toBe('4.2.0');
    expect(RUNTIME_PROFILES['serverless_4.0'].sdpNotes).toContain(
      'Managed Data Pipelines sdp stages'
    );

    expect(RUNTIME_PROFILES['cluster_4.0']).toEqual({
      id: 'cluster_4.0',
      runner: 'DataprocCluster',
      runtimeVersion: '4.0',
      sparkVersion: '4.2.0',
      pythonVersion: '3.13',
      javaVersion: '25',
      scalaVersion: '2.13',
      preinstalledConnectors: [
        'spark-bigquery',
        'iceberg-spark-runtime',
        'gcs-connector',
        'hadoop-aws',
        'hadoop-azure',
      ],
      sdpSupported: true,
      optionalComponents: ['ICEBERG'],
      driverPipRequirements: ['zstandard>=0.25.0'],
      sdpNotes:
        'Dataproc Compute Engine image 4.0 (Spark 4.2) requires --optional-components=ICEBERG and driver package zstandard>=0.25.0.',
    });

    expect(RUNTIME_PROFILES['local_4.2_connect']).toEqual({
      id: 'local_4.2_connect',
      runner: 'Local',
      runtimeVersion: '4.2-connect',
      sparkVersion: '4.2.0',
      pythonVersion: '3.13',
      javaVersion: '21',
      scalaVersion: '2.13',
      preinstalledConnectors: [],
      sdpSupported: true,
      missingLocalIcebergRuntime: true,
      sdpNotes:
        'Local Spark 4.2 Spark Connect profile supports spark-pipelines dry-run and run, but Maven Central lacks iceberg-spark-runtime-4.2_2.13.',
    });
  });

  it('marks runtimes below Apache Spark 4.2 as SDP-incompatible', () => {
    expect(RUNTIME_PROFILES['serverless_3.0'].sdpSupported).toBe(false);
    expect(RUNTIME_PROFILES['serverless_3.0'].sdpNotes).toContain('Apache Spark 4.2+');
    expect(RUNTIME_PROFILES['cluster_3.0'].sdpSupported).toBe(false);
    expect(RUNTIME_PROFILES.local.sdpSupported).toBe(false);
  });

  it('resolves cluster_4.0 and local_4.2_connect while preserving imperative defaults', () => {
    expect(resolveRuntimeProfile('cluster_4.0').id).toBe('cluster_4.0');
    expect(resolveRuntimeProfile('DataprocCluster', '4.0').id).toBe('cluster_4.0');
    expect(resolveRuntimeProfile('DataprocCluster', '4.0-debian13').id).toBe('cluster_4.0');
    expect(resolveRuntimeProfile('DataprocCluster', '4.0 (Spark 4.2)').id).toBe('cluster_4.0');
    expect(resolveRuntimeProfile('dataproc', '4.0').id).toBe('cluster_4.0');
    expect(resolveRuntimeProfile('DataprocCluster').id).toBe('cluster_3.0');
    expect(resolveRuntimeProfile('DataprocCluster', '3.0').id).toBe('cluster_3.0');

    expect(resolveRuntimeProfile('local_4.2_connect').id).toBe('local_4.2_connect');
    expect(resolveRuntimeProfile('Local', '4.2-connect').id).toBe('local_4.2_connect');
    expect(resolveRuntimeProfile('Local', '4.2').id).toBe('local_4.2_connect');
    expect(resolveRuntimeProfile('local', '4.2 (Spark Connect)').id).toBe('local_4.2_connect');
    expect(resolveRuntimeProfile('', '4.2-connect').id).toBe('local_4.2_connect');
    expect(resolveRuntimeProfile('Local').id).toBe('local');
    expect(resolveRuntimeProfile('Local', '4.0').id).toBe('local');
  });

  it('evaluates isRuntimeSdpCapable for RuntimeProfile objects and runner strings', () => {
    expect(isRuntimeSdpCapable(RUNTIME_PROFILES['serverless_4.0'])).toBe(true);
    expect(isRuntimeSdpCapable(RUNTIME_PROFILES['cluster_4.0'])).toBe(true);
    expect(isRuntimeSdpCapable(RUNTIME_PROFILES['local_4.2_connect'])).toBe(true);
    expect(isRuntimeSdpCapable(RUNTIME_PROFILES['serverless_3.0'])).toBe(false);
    expect(isRuntimeSdpCapable(RUNTIME_PROFILES['cluster_3.0'])).toBe(false);
    expect(isRuntimeSdpCapable(RUNTIME_PROFILES.local)).toBe(false);

    expect(isRuntimeSdpCapable('ServerlessSpark')).toBe(true);
    expect(isRuntimeSdpCapable('ServerlessSpark', '4.0')).toBe(true);
    expect(isRuntimeSdpCapable('ServerlessSpark', '3.0')).toBe(false);
    expect(isRuntimeSdpCapable('DataprocCluster')).toBe(false);
    expect(isRuntimeSdpCapable('DataprocCluster', '4.0')).toBe(true);
    expect(isRuntimeSdpCapable('Local')).toBe(false);
    expect(isRuntimeSdpCapable('Local', '4.2-connect')).toBe(true);
    expect(isRuntimeSdpCapable('Local', '4.2')).toBe(true);
    expect(isRuntimeSdpCapable()).toBe(false);
  });

  it('defensively clones optionalComponents and driverPipRequirements arrays', () => {
    const first = resolveRuntimeProfile('cluster_4.0');
    first.optionalComponents?.push('JUPYTER');
    first.driverPipRequirements?.push('mutated-pkg');

    const second = resolveRuntimeProfile('cluster_4.0');
    expect(second.optionalComponents).toEqual(['ICEBERG']);
    expect(second.driverPipRequirements).toEqual(['zstandard>=0.25.0']);
    expect(RUNTIME_PROFILES['cluster_4.0'].optionalComponents).toEqual(['ICEBERG']);
    expect(RUNTIME_PROFILES['cluster_4.0'].driverPipRequirements).toEqual(['zstandard>=0.25.0']);
  });

  it('classifies cluster_4.0 as Managed Spark and keeps local_4.2_connect unmanaged', () => {
    expect(isManagedSparkRuntime('cluster_4.0')).toBe(true);
    expect(isManagedSparkRuntime('local_4.2_connect')).toBe(false);

    const clusterIceberg = resolveConnectorForRuntime('IcebergSink', 'cluster_4.0');
    expect(clusterIceberg.mechanism).toBe('preinstalled');
    expect(clusterIceberg.preinstalledArtifact).toBe('iceberg-spark-runtime');
    expect(clusterIceberg.mavenCoordinates).toEqual([]);
  });

  it('flags missing local Iceberg runtime when IcebergSource or IcebergSink runs on local_4.2_connect', () => {
    for (const transform of ['IcebergSource', 'IcebergSink', 'IcebergIO']) {
      const resolution = resolveConnectorForRuntime(transform, 'local_4.2_connect', {
        warehouse: 'gs://lakehouse-bucket/warehouse',
      });
      expect(resolution.missingLocalIcebergRuntime).toBe(true);
      expect(resolution.notes).toContain('iceberg-spark-runtime-4.2_2.13');
      expect(resolution.mavenCoordinates).toEqual([
        'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25',
      ]);
    }

    const artifactsWithIceberg = resolvePipelineRuntimeArtifacts(
      [{ sparkTransform: 'CsvSource' }, { sparkTransform: 'IcebergSink' }],
      { runner: 'Local', runtimeVersion: '4.2-connect', paradigm: 'sdp' }
    );
    expect(artifactsWithIceberg.profile.id).toBe('local_4.2_connect');
    expect(artifactsWithIceberg.missingLocalIcebergRuntime).toBe(true);

    const artifactsWithoutIceberg = resolvePipelineRuntimeArtifacts(
      [{ sparkTransform: 'CsvSource' }, { sparkTransform: 'ParquetSink' }],
      { runner: 'Local', runtimeVersion: '4.2-connect', paradigm: 'sdp' }
    );
    expect(artifactsWithoutIceberg.profile.id).toBe('local_4.2_connect');
    expect(artifactsWithoutIceberg.missingLocalIcebergRuntime).toBeUndefined();
  });

  it('attaches driverPipRequirements to pipPackages when resolving cluster_4.0 in SDP mode', () => {
    const sdpClusterArtifacts = resolvePipelineRuntimeArtifacts(
      [{ sparkTransform: 'IcebergSink' }],
      { runner: 'DataprocCluster', runtimeVersion: '4.0', paradigm: 'sdp' }
    );
    expect(sdpClusterArtifacts.profile.id).toBe('cluster_4.0');
    expect(sdpClusterArtifacts.pipPackages).toContain('zstandard>=0.25.0');

    const imperativeClusterArtifacts = resolvePipelineRuntimeArtifacts(
      [{ sparkTransform: 'IcebergSink' }],
      { runner: 'DataprocCluster', runtimeVersion: '4.0' }
    );
    expect(imperativeClusterArtifacts.profile.id).toBe('cluster_4.0');
    expect(imperativeClusterArtifacts.pipPackages).toEqual([]);
  });
});
