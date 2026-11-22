/**
 * @fileoverview Test utilities and mock context factory for BottomPanel and its child tabs.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React from 'react';
import { render, type RenderResult } from '@testing-library/react';
import { vi } from 'vitest';
import {
  BottomPanelProvider,
  type BottomPanelContextValue,
} from './BottomPanelContext';
import { pipelineConfigReducer, type PipelineConfig } from '../../state/pipelineConfigReducer';
import type { LogEntry } from './DeploymentTab';
import type { TokenUsage } from './SourceCodeTab';
import type { CodegenModelOption } from '../../utils/sharedTypes';

type JobConfigSlice = BottomPanelContextValue['jobConfig'];
type RuntimeVariablesSlice = BottomPanelContextValue['runtimeVariables'];
type PipelineConfigSlice = BottomPanelContextValue['pipelineConfig'];
type CodegenSlice = BottomPanelContextValue['codegen'];
type SetPipelineConfigFn = NonNullable<PipelineConfigSlice['setPipelineConfig']>;

/**
 * Checks whether an override object matches the nested `jobConfig` context slice shape.
 */
function isJobConfigSliceOverride(
  input: unknown
): input is Partial<JobConfigSlice> & { value: JobConfigSlice['value'] } {
  return Boolean(input && typeof input === 'object' && 'value' in input);
}

/**
 * Checks whether an override object matches the nested `runtimeVariables` context slice shape.
 */
function isRuntimeVariablesSliceOverride(
  input: unknown
): input is Partial<RuntimeVariablesSlice> & { variables?: RuntimeVariablesSlice['variables'] } {
  return Boolean(
    input && typeof input === 'object' && !Array.isArray(input) && 'variables' in input
  );
}

/**
 * Checks whether an override object matches the nested `pipelineConfig` context slice shape.
 */
function isPipelineConfigSliceOverride(
  input: unknown
): input is Partial<PipelineConfigSlice> {
  return Boolean(input && typeof input === 'object' && 'config' in input);
}

/**
 * Optional slice and convenience overrides accepted by {@link createBottomPanelContextValue}.
 */
export interface BottomPanelTestOverrides {
  // Direct slices
  execution?: Partial<BottomPanelContextValue['execution']>;
  jobConfig?: Partial<JobConfigSlice> | Record<string, unknown>;
  runtimeVariables?: Partial<RuntimeVariablesSlice> | { key: string; value: string }[];
  pipelineConfig?: Partial<PipelineConfigSlice> | PipelineConfig;
  codegen?: Partial<CodegenSlice>;

  // Flat convenience fields
  setJobConfig?: JobConfigSlice['set'];
  addVariable?: () => void;
  updateVariable?: (index: number, field: 'key' | 'value', value: string) => void;
  deleteVariable?: (index: number) => void;
  isJobIdValid?: (value: string) => boolean;
  onRunNow?: () => void;
  onStopPipeline?: (options?: { cancelRemoteJob?: boolean }) => void;
  subnetworks?: { name: string; region: string; network: string; value: string }[];
  clusters?: { name: string; status?: string; region?: string }[];
  sessionTemplates?: Array<{ name: string; value?: string; description?: string } | string>;
  needsRebuild?: boolean;
  pipelineLogs?: LogEntry[];
  pipelineStatus?: 'idle' | 'running' | 'completed' | 'failed' | 'stopped';
  runnerCommand?: string;
  onClearLogs?: () => void;
  onOpenConsole?: (url: string) => void;
  projectId?: string;
  generatedCode?: string;
  language?: 'Python' | 'Java';
  setLanguage?: (lang: 'Python' | 'Java') => void;
  outputFolder?: string;
  setOutputFolder?: (folder: string) => void;
  setPipelineConfig?: SetPipelineConfigFn;
  onPickFolder?: () => void;
  onRegenerate?: (options?: { enableReviewer?: boolean; model?: string }) => void;
  onStopGeneration?: () => void;
  files?: string[];
  folderFiles?: string[];
  selectedFile?: string | null;
  selectedFileName?: string | null;
  onSelectFile?: (fileName: string) => void;
  selectedFileContent?: string | null;
  isGenerating?: boolean;
  generationProgress?: string | null;
  isGraphValid?: boolean;
  validationErrors?: string[];
  nodes?: CodegenSlice['nodes'];
  tokenUsage?: TokenUsage | null;
  enableReviewer?: boolean;
  setEnableReviewer?: (value: boolean) => void;
  selectedModel?: string;
  setSelectedModel?: (model: string) => void;
  models?: CodegenModelOption[];
  modelsLoading?: boolean;
  modelsError?: string | null;
  refreshModels?: () => void;
}

/**
 * Creates a fully-populated BottomPanelContextValue with sensible defaults and applied overrides.
 */
export function createBottomPanelContextValue(
  overrides?: BottomPanelTestOverrides
): BottomPanelContextValue {
  const pipelineSliceOverride = isPipelineConfigSliceOverride(overrides?.pipelineConfig)
    ? overrides.pipelineConfig
    : undefined;
  const setPipelineConfig =
    overrides?.setPipelineConfig ?? pipelineSliceOverride?.setPipelineConfig;

  const rawConfig: Partial<PipelineConfig> =
    (pipelineSliceOverride
      ? pipelineSliceOverride.config
      : (overrides?.pipelineConfig as Partial<PipelineConfig> | undefined)) ?? {};

  const config: PipelineConfig = {
    language: overrides?.language ?? rawConfig.language ?? 'Python',
    outputFolder: overrides?.outputFolder ?? rawConfig.outputFolder ?? '',
    needsRebuild: overrides?.needsRebuild ?? rawConfig.needsRebuild ?? false,
  };

  const jobSliceOverride = isJobConfigSliceOverride(overrides?.jobConfig)
    ? overrides.jobConfig
    : undefined;
  const rawJobConfig = (
    jobSliceOverride
      ? jobSliceOverride.value
      : (overrides?.jobConfig ?? { runner: 'Local' })
  ) as JobConfigSlice['value'];

  const rvSliceOverride = isRuntimeVariablesSliceOverride(overrides?.runtimeVariables)
    ? overrides.runtimeVariables
    : undefined;
  const rawVariables =
    (rvSliceOverride
      ? rvSliceOverride.variables
      : Array.isArray(overrides?.runtimeVariables)
        ? overrides.runtimeVariables
        : []) ?? [];

  const rawExec = overrides?.execution ?? {};
  const rawJob: Partial<JobConfigSlice> = jobSliceOverride ?? {};
  const rawRv: Partial<RuntimeVariablesSlice> = rvSliceOverride ?? {};
  const rawCodegen = overrides?.codegen ?? {};

  return {
    execution: {
      logs: rawExec.logs ?? overrides?.pipelineLogs ?? [],
      status: rawExec.status ?? overrides?.pipelineStatus ?? 'idle',
      runnerCommand: rawExec.runnerCommand ?? overrides?.runnerCommand ?? '',
      run: rawExec.run ?? overrides?.onRunNow ?? vi.fn(),
      stop: rawExec.stop ?? overrides?.onStopPipeline ?? vi.fn(),
      clear: rawExec.clear ?? overrides?.onClearLogs ?? vi.fn(),
      openConsole: rawExec.openConsole ?? overrides?.onOpenConsole ?? vi.fn(),
    },
    jobConfig: {
      value: rawJobConfig,
      set: rawJob.set ?? overrides?.setJobConfig ?? vi.fn(),
      isJobIdValid: rawJob.isJobIdValid ?? overrides?.isJobIdValid ?? vi.fn().mockReturnValue(true),
      subnetworks: rawJob.subnetworks ?? overrides?.subnetworks ?? [],
      clusters: rawJob.clusters ?? overrides?.clusters,
      sessionTemplates: rawJob.sessionTemplates ?? overrides?.sessionTemplates,
      projectId: rawJob.projectId ?? overrides?.projectId,
    },
    runtimeVariables: {
      variables: rawVariables,
      addVariable: rawRv.addVariable ?? overrides?.addVariable ?? vi.fn(),
      updateVariable: rawRv.updateVariable ?? overrides?.updateVariable ?? vi.fn(),
      deleteVariable: rawRv.deleteVariable ?? overrides?.deleteVariable ?? vi.fn(),
    },
    pipelineConfig: {
      config,
      setLanguage:
        overrides?.setLanguage ??
        pipelineSliceOverride?.setLanguage ??
        ((lang) =>
          setPipelineConfig?.((prev: PipelineConfig) =>
            pipelineConfigReducer(prev, { type: 'setLanguage', language: lang })
          )),
      setOutputFolder:
        overrides?.setOutputFolder ??
        pipelineSliceOverride?.setOutputFolder ??
        ((folder) =>
          setPipelineConfig?.((prev: PipelineConfig) =>
            pipelineConfigReducer(prev, { type: 'setOutputFolder', folder })
          )),
      setPipelineConfig,
    },
    codegen: {
      generatedCode: rawCodegen.generatedCode ?? overrides?.generatedCode ?? '',
      regenerate: rawCodegen.regenerate ?? overrides?.onRegenerate ?? vi.fn(),
      stopGeneration: rawCodegen.stopGeneration ?? overrides?.onStopGeneration,
      isGenerating: rawCodegen.isGenerating ?? overrides?.isGenerating ?? false,
      generationProgress: rawCodegen.generationProgress ?? overrides?.generationProgress ?? null,
      files: rawCodegen.files ?? overrides?.files ?? overrides?.folderFiles ?? [],
      selectedFile: rawCodegen.selectedFile ?? overrides?.selectedFile ?? overrides?.selectedFileName ?? null,
      selectedFileContent: rawCodegen.selectedFileContent ?? overrides?.selectedFileContent ?? null,
      pickFolder: rawCodegen.pickFolder ?? overrides?.onPickFolder ?? vi.fn(),
      selectFile: rawCodegen.selectFile ?? overrides?.onSelectFile ?? vi.fn(),
      isGraphValid: rawCodegen.isGraphValid ?? overrides?.isGraphValid ?? true,
      validationErrors: rawCodegen.validationErrors ?? overrides?.validationErrors ?? [],
      nodes: rawCodegen.nodes ?? overrides?.nodes ?? [],
      tokenUsage: rawCodegen.tokenUsage ?? overrides?.tokenUsage ?? null,
      useSdkForUnsupportedNodes: rawCodegen.useSdkForUnsupportedNodes ?? false,
      setUseSdkForUnsupportedNodes: rawCodegen.setUseSdkForUnsupportedNodes ?? vi.fn(),
      enableReviewer: rawCodegen.enableReviewer ?? overrides?.enableReviewer,
      setEnableReviewer: rawCodegen.setEnableReviewer ?? overrides?.setEnableReviewer,
      selectedModel: rawCodegen.selectedModel ?? overrides?.selectedModel,
      setSelectedModel: rawCodegen.setSelectedModel ?? overrides?.setSelectedModel,
      models: rawCodegen.models ?? overrides?.models,
      modelsLoading: rawCodegen.modelsLoading ?? overrides?.modelsLoading,
      modelsError: rawCodegen.modelsError ?? overrides?.modelsError,
      refreshModels: rawCodegen.refreshModels ?? overrides?.refreshModels,
    },
  };
}

/**
 * Result returned by {@link renderWithBottomPanel}, exposing the active context and rerender helper.
 */
export interface RenderWithBottomPanelResult extends RenderResult {
  contextValue: BottomPanelContextValue;
  rerenderWithContext: (
    ui: React.ReactElement,
    newOverrides?: BottomPanelTestOverrides
  ) => void;
}

/**
 * Test helper that renders a component inside BottomPanelProvider with mock/custom context.
 */
export function renderWithBottomPanel(
  ui: React.ReactElement,
  overrides?: BottomPanelTestOverrides
): RenderWithBottomPanelResult {
  let contextValue = createBottomPanelContextValue(overrides);

  const result = render(
    <BottomPanelProvider value={contextValue}>{ui}</BottomPanelProvider>
  );

  const rerenderWithContext = (
    newUi: React.ReactElement,
    newOverrides?: BottomPanelTestOverrides
  ) => {
    if (newOverrides !== undefined) {
      contextValue = createBottomPanelContextValue(newOverrides);
    }
    result.rerender(
      <BottomPanelProvider value={contextValue}>{newUi}</BottomPanelProvider>
    );
  };

  return {
    ...result,
    contextValue,
    rerenderWithContext,
  };
}
