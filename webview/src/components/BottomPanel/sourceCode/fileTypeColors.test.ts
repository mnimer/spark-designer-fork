/**
 * @fileoverview Unit tests for generated file color badges and descriptions.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import { getFileColorInfo, getFileDescription } from './fileTypeColors';

describe('fileTypeColors', () => {
  it('classifies Python, Spark runner scripts, SDP specs, and test suites (normal condition)', () => {
    expect(getFileColorInfo('src/pipeline.py').category).toBe('python');
    expect(getFileColorInfo('tests/test_pipeline.py').category).toBe('test');
    expect(getFileColorInfo('run_local.sh').category).toBe('script');
    expect(getFileColorInfo('spark-pipeline.yml').category).toBe('config');
    expect(getFileColorInfo('requirements.txt').category).toBe('deps');
    expect(getFileColorInfo('Dockerfile').category).toBe('docker');
    expect(getFileColorInfo('README.md').category).toBe('docs');

    expect(getFileDescription('src/pipeline.py')).toBe('PySpark pipeline implementation');
    expect(getFileDescription('run_local.sh')).toBe('Local PySpark execution script');
    expect(getFileDescription('run_dataproc_cluster.sh')).toBe(
      'Google Cloud Dataproc Cluster submission script'
    );
    expect(getFileDescription('run_serverless_spark.sh')).toBe(
      'Google Cloud Serverless Spark batch submission script'
    );
    expect(getFileDescription('spark-pipeline.yml')).toBe(
      'Spark Declarative Pipeline specification'
    );
    expect(getFileDescription('sdp_stage.pipeline.yaml')).toBe(
      'Managed Data Pipelines SDP stage definition'
    );
  });

  it('normalizes Windows backslashes and dotfile labels (boundary condition)', () => {
    expect(getFileColorInfo('src\\transforms.py').category).toBe('python');
    expect(getFileColorInfo('.gcloudignore').badgeLabel).toBe('GCLOUDIGNORE');
    expect(getFileDescription('src\\__init__.py')).toBe(
      'Python package initialization marker'
    );
  });

  it('falls back safely on empty or unknown filenames (failure condition)', () => {
    expect(getFileColorInfo('').badgeLabel).toBe('FILE');
    expect(getFileDescription('custom.bin')).toBe('custom.bin file');
  });
});
