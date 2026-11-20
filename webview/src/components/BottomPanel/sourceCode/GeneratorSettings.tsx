/**
 * @fileoverview Generator settings panel for configuring PySpark 4.x runtime badge, Gemini model selector, output folder, AI reviewer pass toggle, and execution controls.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Architectural Decisions:
 * - PySpark-only in v1 (S6, G12): replaces the legacy Python/Java radio selector with a read-only
 *   `PySpark (Spark 4.x)` runtime badge (`data-testid="pyspark-runtime-badge"`).
 * - Exposes dynamic Gemini code generation model selection (`data-testid="codegen-model-select"`)
 *   with manual refresh and offline fallback to `gemini-4-hydrogen`.
 * - Exposes the opt-in AI Code Reviewer Pass toggle (`data-testid="ai-reviewer-toggle"`),
 *   defaulting to false (`CODEGEN_REVIEWER_ENABLED_BY_DEFAULT = false`) and forwarding the
 *   selected boolean and model when invoking `onRegenerate({ enableReviewer, model })`.
 * - Follows the 8pt grid spacing and semantic VS Code CSS variable theming from DESIGN.md.
 */

import React from 'react';
import {
  Sliders,
  Folder,
  CheckCircle2,
  AlertCircle,
  RefreshCw,
  Play,
  Square,
  ShieldCheck,
  Sparkles,
} from 'lucide-react';

import { InfoTooltip } from '../../InfoTooltip';
import { useAssetLoader } from '../../../hooks/useAssetLoader';
import type { CodegenModelOption, TokenUsage } from '../../../utils/sharedTypes';

/** Default state for the optional AI code reviewer pass (matches `CODEGEN_REVIEWER_ENABLED_BY_DEFAULT`). */
export const CODEGEN_REVIEWER_ENABLED_BY_DEFAULT = false;

/** Display label for the read-only PySpark 4.x runtime badge. */
export const PYSPARK_RUNTIME_BADGE_LABEL = 'PySpark (Spark 4.x)';

const FALLBACK_MODEL_OPTION: CodegenModelOption = {
  id: 'gemini-4-hydrogen',
  displayName: 'Gemini 4 Hydrogen',
  tier: 'flash',
  version: '4.0',
  isDefault: true,
};

/**
 * Resolves the highest-ranked Flash-class default model ID from discovered catalog options.
 */
function resolveCatalogDefaultModelId(options: CodegenModelOption[]): string {
  if (options.length === 0) {
    return FALLBACK_MODEL_OPTION.id;
  }
  return (
    options.find((m) => m.isDefault)?.id ??
    options.find((m) => m.tier === 'flash')?.id ??
    options[0]?.id ??
    FALLBACK_MODEL_OPTION.id
  );
}

/**
 * Builds a non-empty model option list for the selector and preserves controlled values.
 */
function buildSelectableModelOptions(
  options: CodegenModelOption[],
  activeModelId: string,
): CodegenModelOption[] {
  const baseOptions = options.length > 0 ? options : [FALLBACK_MODEL_OPTION];
  const trimmedActive = activeModelId.trim();
  if (!trimmedActive || baseOptions.some((m) => m.id === trimmedActive)) {
    return baseOptions;
  }
  return [
    ...baseOptions,
    {
      id: trimmedActive,
      displayName: trimmedActive,
      tier: 'other',
    },
  ];
}

/**
 * Metadata for a diagram node lacking a built-in native connector in the target language.
 */
export interface UnsupportedDiagramNode {
  id: string;
  label: string;
  sparkTransform: string;
}

export type { TokenUsage };

/**
 * Props for the GeneratorSettings component.
 */
export interface GeneratorSettingsProps {
  /** Target programming language for pipeline generation (retained for backward compatibility). */
  language?: 'Python' | 'Java';
  /** Optional callback invoked when the target language changes. */
  setLanguage?: (lang: 'Python' | 'Java') => void;
  /** Current output folder path string. */
  outputFolder: string;
  /** Callback invoked when the user edits the output folder path. */
  setOutputFolder: (folder: string) => void;
  /** Callback invoked when browsing for a folder. */
  onPickFolder: () => void;
  /** Callback invoked when requesting pipeline code generation. */
  onRegenerate: (options?: { enableReviewer?: boolean; model?: string }) => void;
  /** Optional callback invoked to abort active generation. */
  onStopGeneration?: () => void;
  /** Whether code generation is currently in progress. */
  isGenerating?: boolean;
  /** Whether the diagram topology and language support are valid. */
  effectiveIsGraphValid: boolean;
  /** List of validation error strings preventing generation. */
  effectiveValidationErrors: string[];
  /** Nodes without a native connector in the target language (empty when enforcement is off). */
  unsupportedNodes?: UnsupportedDiagramNode[];
  /** Whether unsupported nodes may use the SDK workaround instead of blocking generation. */
  useSdkForUnsupportedNodes?: boolean;
  /** Callback invoked when the user toggles the SDK workaround checkbox. */
  onUseSdkForUnsupportedNodesChange?: (value: boolean) => void;
  /** Whether the optional post-generation AI code reviewer pass is enabled. */
  enableReviewer?: boolean;
  /** Callback invoked when the user toggles the AI code reviewer pass. */
  onEnableReviewerChange?: (enabled: boolean) => void;
  /** Sanitized package or directory name displayed for validation. */
  packageDisplayName: string;
  /** Optional token usage details from the generation model. */
  tokenUsage?: TokenUsage | null;
  /** Currently selected Gemini model identifier for code generation. */
  selectedModel?: string;
  /** Callback invoked when the user selects or auto-resolves a Gemini model. */
  onSelectedModelChange?: (model: string) => void;
  /** Discovered Gemini code generation model options from the host catalog. */
  models?: CodegenModelOption[];
  /** Whether Gemini model discovery is currently in flight. */
  modelsLoading?: boolean;
  /** Optional warning or error message from Gemini model discovery. */
  modelsError?: string | null;
  /** Optional callback invoked to refresh available Gemini models. */
  onRefreshModels?: () => void;
}

/**
 * GeneratorSettings component rendering the left column configuration controls.
 *
 * @param props Component properties.
 * @returns React component.
 */
export const GeneratorSettings: React.FC<GeneratorSettingsProps> = ({
  language = 'Python',
  outputFolder,
  setOutputFolder,
  onPickFolder,
  onRegenerate,
  onStopGeneration,
  isGenerating = false,
  effectiveIsGraphValid,
  effectiveValidationErrors,
  unsupportedNodes = [],
  useSdkForUnsupportedNodes = false,
  onUseSdkForUnsupportedNodesChange,
  enableReviewer = CODEGEN_REVIEWER_ENABLED_BY_DEFAULT,
  onEnableReviewerChange,
  packageDisplayName,
  tokenUsage,
  selectedModel,
  onSelectedModelChange,
  models,
  modelsLoading,
  modelsError,
  onRefreshModels,
}) => {
  const [localSelectedModel, setLocalSelectedModel] = React.useState<string | undefined>(undefined);
  const userSelectedModelRef = React.useRef<boolean>(false);
  const currentSelectedModelRef = React.useRef<string>(
    selectedModel?.trim() || FALLBACK_MODEL_OPTION.id,
  );
  const onSelectedModelChangeRef = React.useRef(onSelectedModelChange);

  const standaloneLoader = useAssetLoader<CodegenModelOption>('codegenModels', {
    enabled: models === undefined,
    onLoaded: (loadedItems) => {
      if (loadedItems.length === 0) {
        return;
      }
      const nextDefault = resolveCatalogDefaultModelId(loadedItems);
      const currentModel = currentSelectedModelRef.current;
      const existsInCatalog = loadedItems.some((m) => m.id === currentModel);
      if (!userSelectedModelRef.current || !existsInCatalog) {
        userSelectedModelRef.current = false;
        currentSelectedModelRef.current = nextDefault;
        setLocalSelectedModel(nextDefault);
        onSelectedModelChangeRef.current?.(nextDefault);
      }
    },
  });

  const resolvedModels = models ?? standaloneLoader.items;
  const effectiveModelsLoading =
    modelsLoading ?? (models === undefined ? standaloneLoader.loading : false);
  const effectiveModelsError =
    modelsError !== undefined ? modelsError : standaloneLoader.error;

  const catalogDefaultModel = React.useMemo(
    () => resolveCatalogDefaultModelId(resolvedModels),
    [resolvedModels],
  );

  const effectiveSelectedModel =
    (selectedModel && selectedModel.trim()) ||
    localSelectedModel ||
    catalogDefaultModel;

  React.useEffect(() => {
    currentSelectedModelRef.current = effectiveSelectedModel;
    onSelectedModelChangeRef.current = onSelectedModelChange;
  }, [effectiveSelectedModel, onSelectedModelChange]);

  const selectableModels = React.useMemo(
    () => buildSelectableModelOptions(resolvedModels, effectiveSelectedModel),
    [resolvedModels, effectiveSelectedModel],
  );

  const handleModelChange = (e: React.ChangeEvent<HTMLSelectElement>): void => {
    const nextModel = e.target.value;
    userSelectedModelRef.current = true;
    currentSelectedModelRef.current = nextModel;
    setLocalSelectedModel(nextModel);
    onSelectedModelChange?.(nextModel);
  };

  const handleRefreshModels = (): void => {
    if (onRefreshModels) {
      onRefreshModels();
      return;
    }
    standaloneLoader.refresh();
  };

  const handleOutputFolderChange = (e: React.ChangeEvent<HTMLInputElement>): void => {
    setOutputFolder(e.target.value);
  };

  const handleReviewerToggle = (e: React.ChangeEvent<HTMLInputElement>): void => {
    onEnableReviewerChange?.(e.target.checked);
  };

  const handleUseSdkChange = (e: React.ChangeEvent<HTMLInputElement>): void => {
    onUseSdkForUnsupportedNodesChange?.(e.target.checked);
  };

  const handleGenerateClick = (): void => {
    if (!isGenerating && effectiveIsGraphValid) {
      onRegenerate({ enableReviewer, model: effectiveSelectedModel });
    }
  };

  return (
    <div className="w-76 shrink-0 flex flex-col bg-[var(--vscode-card-bg,#161b22)] rounded-xl border border-[var(--vscode-border,#30363d)] overflow-hidden shadow-lg">
      {/* Panel Header */}
      <div className="border-b border-[var(--vscode-border,#30363d)] bg-[var(--vscode-subcard-bg,#0d1117)] py-2 px-4 flex items-center">
        <div className="flex items-center gap-2">
          <Sliders className="w-3.5 h-3.5 text-blue-400 shrink-0" />
          <span className="text-xs font-bold tracking-wider text-[var(--vscode-text-primary,#f0f6fc)] uppercase">
            GENERATOR SETTINGS
          </span>
        </div>
      </div>

      {/* Panel Content */}
      <div className="flex-1 flex flex-col p-4 gap-4 overflow-y-auto">
        {/* Target Runtime Badge (Read-only PySpark Spark 4.x) */}
        <div className="flex flex-col gap-2">
          <div className="flex items-center justify-between">
            <span className="text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted,#8b949e)]">
              TARGET RUNTIME
            </span>
            <span className="text-[10px] font-bold uppercase tracking-wider text-emerald-400 font-mono">
              ACTIVE
            </span>
          </div>
          <div
            data-testid="pyspark-runtime-badge"
            className="flex items-center justify-between px-3 py-2 rounded-lg bg-[var(--vscode-subcard-bg,#0d1117)] border border-[var(--vscode-border,#30363d)]"
          >
            <div className="flex items-center gap-2">
              <Sparkles className="w-3.5 h-3.5 text-amber-400 shrink-0" />
              <span className="text-xs font-bold text-[var(--vscode-text-primary,#f0f6fc)] font-mono">
                {PYSPARK_RUNTIME_BADGE_LABEL}
              </span>
            </div>
            <span className="px-1.5 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider bg-blue-500/10 border border-blue-500/30 text-blue-400 font-mono">
              Python 3.12+
            </span>
          </div>
        </div>

        {/* Codegen Model Selector */}
        <div className="flex flex-col gap-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-1">
              <label
                htmlFor="codegen-model-select"
                className="text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted,#8b949e)]"
              >
                CODEGEN MODEL
              </label>
              <InfoTooltip text="Selects the Gemini model used for PySpark code generation and optional AI review. Defaults to the latest available Flash-class model." />
            </div>
            <button
              type="button"
              data-testid="refresh-codegen-models-btn"
              aria-label="Refresh Gemini models"
              title="Refresh available Gemini models"
              onClick={handleRefreshModels}
              disabled={isGenerating || effectiveModelsLoading}
              className="p-1 rounded text-[var(--vscode-text-secondary,#c9d1d9)] hover:text-[var(--vscode-text-primary,#f0f6fc)] hover:bg-[var(--vscode-hover,#30363d)] disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer transition"
            >
              <RefreshCw
                className={`w-3.5 h-3.5 ${effectiveModelsLoading ? 'animate-spin' : ''}`}
              />
            </button>
          </div>

          <select
            id="codegen-model-select"
            data-testid="codegen-model-select"
            value={effectiveSelectedModel}
            onChange={handleModelChange}
            disabled={isGenerating}
            className="bg-[var(--vscode-input-bg,#0d1117)] text-[var(--vscode-text-primary,#f0f6fc)] px-3 py-2 rounded-lg border border-[var(--vscode-border,#30363d)] focus:outline-none focus:border-[var(--vscode-highlight,#58a6ff)] text-xs w-full font-mono shadow-inner transition disabled:opacity-60 disabled:cursor-not-allowed"
          >
            {selectableModels.map((m) => (
              <option key={m.id} value={m.id}>
                {m.displayName && m.displayName !== m.id
                  ? `${m.displayName} (${m.id})`
                  : m.id}
              </option>
            ))}
          </select>

          {effectiveModelsError && (
            <div
              data-testid="codegen-models-error"
              className="flex items-center gap-1.5 text-amber-400 text-[11px]"
            >
              <AlertCircle className="w-3.5 h-3.5 shrink-0 text-amber-400" />
              <span className="truncate">{effectiveModelsError}</span>
            </div>
          )}
        </div>

        {/* Target Folder Picker */}
        <div className="flex flex-col gap-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-1.5">
              <Folder className="w-3.5 h-3.5 text-amber-400 shrink-0" />
              <label
                htmlFor="generator-target-folder-input"
                className="text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted,#8b949e)]"
              >
                TARGET FOLDER
              </label>
            </div>
            <button
              type="button"
              onClick={onPickFolder}
              className="text-xs font-medium text-blue-400 hover:text-blue-300 cursor-pointer hover:underline transition"
            >
              Paths
            </button>
          </div>

          <div className="flex gap-2 items-center">
            <input
              id="generator-target-folder-input"
              type="text"
              value={outputFolder}
              onChange={handleOutputFolderChange}
              placeholder="./scratch_flow/"
              className="bg-[var(--vscode-input-bg,#0d1117)] text-[var(--vscode-text-primary,#f0f6fc)] px-3 py-2 rounded-lg border border-[var(--vscode-border,#30363d)] focus:outline-none focus:border-[var(--vscode-highlight,#58a6ff)] text-xs flex-1 min-w-0 font-mono shadow-inner transition"
            />
            <button
              type="button"
              onClick={onPickFolder}
              className="bg-[var(--vscode-button-secondary,#21262d)] hover:bg-[var(--vscode-hover,#30363d)] text-[var(--vscode-text-secondary,#c9d1d9)] hover:text-[var(--vscode-text-primary,#f0f6fc)] p-2 rounded-lg border border-[var(--vscode-border,#30363d)] flex items-center justify-center shrink-0 cursor-pointer transition shadow-sm"
              title="Browse Folder"
            >
              <Folder className="w-4 h-4 text-[var(--vscode-text-secondary,#c9d1d9)]" />
            </button>
          </div>

          {/* Validation subtitle */}
          <div className="flex items-center gap-1.5 text-emerald-400 text-[11px] font-mono">
            <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400 shrink-0" />
            <span className="truncate">Valid PySpark package name: {packageDisplayName}</span>
          </div>
        </div>

        {/* AI Code Reviewer Pass Toggle */}
        <div
          data-testid="ai-reviewer-pass-section"
          className="flex flex-col gap-1.5 p-3 rounded-lg bg-[var(--vscode-subcard-bg,#0d1117)] border border-[var(--vscode-border,#30363d)]"
        >
          <label className="flex items-center justify-between gap-2 cursor-pointer select-none">
            <div className="flex items-center gap-2 min-w-0">
              <ShieldCheck className="w-3.5 h-3.5 text-emerald-400 shrink-0" />
              <span className="text-xs font-bold text-[var(--vscode-text-primary,#f0f6fc)] truncate">
                AI Code Reviewer Pass
              </span>
            </div>
            <input
              type="checkbox"
              role="switch"
              aria-checked={enableReviewer}
              aria-label="AI Code Reviewer Pass"
              data-testid="ai-reviewer-toggle"
              checked={enableReviewer}
              onChange={handleReviewerToggle}
              disabled={isGenerating}
              className="accent-emerald-500 w-4 h-4 cursor-pointer shrink-0 disabled:cursor-not-allowed"
            />
          </label>
          <p className="text-[11px] text-[var(--vscode-text-muted,#8b949e)] leading-relaxed">
            Runs read-only syntax and pytest checks after generation and asks Gemini to fix blocking defects.
          </p>
        </div>

        {/* Topological DAG & SDK Language Compatibility Validation Error Callout */}
        {!effectiveIsGraphValid && effectiveValidationErrors.length > 0 && (
          <div
            className="p-3 bg-rose-950/60 border border-rose-800/80 rounded-lg flex items-start gap-2 text-xs text-rose-200"
            data-testid="codegen-validation-error"
          >
            <AlertCircle className="w-4 h-4 text-rose-400 shrink-0 mt-0.5" />
            <div className="flex-1 min-w-0">
              <span className="font-semibold block text-rose-300">Cannot Generate Code</span>
              {effectiveValidationErrors.length === 1 ? (
                <span className="text-[11px] text-rose-200/90 leading-relaxed block break-words mt-1">
                  {effectiveValidationErrors[0]}
                </span>
              ) : (
                <ul className="text-[11px] text-rose-200/90 leading-relaxed mt-1 list-disc list-inside space-y-1">
                  {effectiveValidationErrors.map((err, idx) => (
                    <li key={idx} className="break-words">{err}</li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        )}

        {/* SDK workaround opt-in: shown whenever nodes lack a native connector in the
            target language. Checking it removes the language errors above and lets the
            codegen skills generate an SDK-based implementation for those nodes. */}
        {unsupportedNodes.length > 0 && onUseSdkForUnsupportedNodesChange && (
          <div data-testid="use-sdk-for-unsupported">
            <label className="flex items-start gap-2 text-[11px] text-[var(--vscode-text-secondary,#c9d1d9)] cursor-pointer select-none">
              <input
                type="checkbox"
                className="mt-0.5 accent-amber-500 cursor-pointer"
                checked={useSdkForUnsupportedNodes}
                onChange={handleUseSdkChange}
              />
              <span>
                Generate {unsupportedNodes.length === 1 ? 'this node' : 'these nodes'} with the {language} SDK instead of a built-in connector
              </span>
            </label>
            {useSdkForUnsupportedNodes && (
              <p className="mt-1 ml-5 text-[11px] text-amber-300/90 leading-relaxed" data-testid="use-sdk-notice">
                No built-in {language} connector for {unsupportedNodes.map((n) => n.label).join(', ')}. Gemini will write custom
                SDK code (for example a Python DataSource or foreachBatch sink). Review it before you deploy.
              </p>
            )}
          </div>
        )}

        {/* Action Buttons: Generate Code & Stop */}
        <div className="mt-auto pt-3 border-t border-[var(--vscode-border,#30363d)] flex flex-col gap-2">
          <div className="flex gap-2">
            <button
              type="button"
              onClick={handleGenerateClick}
              disabled={isGenerating || !effectiveIsGraphValid}
              title={
                !effectiveIsGraphValid && effectiveValidationErrors.length > 0
                  ? `Cannot Generate Code:\n${effectiveValidationErrors.join('\n')}`
                  : undefined
              }
              className={`flex items-center justify-center gap-2 py-2 px-4 rounded-lg text-xs font-bold flex-1 transition shadow-lg shadow-emerald-950/40 ${
                isGenerating || !effectiveIsGraphValid
                  ? 'bg-slate-800 text-slate-500 cursor-not-allowed shadow-none opacity-80'
                  : 'bg-[#059669] hover:bg-[#10b981] active:bg-[#047857] text-white active:scale-[0.98] cursor-pointer'
              }`}
            >
              {isGenerating ? (
                <RefreshCw className="w-3.5 h-3.5 animate-spin" />
              ) : (
                <Play className="w-3.5 h-3.5 fill-current" />
              )}
              <span>{isGenerating ? 'Generating...' : 'Generate Code'}</span>
            </button>
            <button
              type="button"
              onClick={onStopGeneration}
              disabled={!isGenerating && !onStopGeneration}
              className={`flex items-center justify-center gap-1.5 py-2 px-3 rounded-lg text-xs font-bold transition shadow-sm ${
                isGenerating
                  ? 'bg-[#881337] hover:bg-[#9f1239] active:bg-[#4c0519] border border-rose-800/80 text-rose-200 hover:text-white cursor-pointer active:scale-[0.98]'
                  : 'bg-[#881337]/70 text-rose-300/80 border border-rose-900/40 cursor-pointer hover:bg-[#881337] hover:text-rose-100'
              }`}
              title="Stop Code Generation"
            >
              <Square className="w-3.5 h-3.5 fill-current" />
              <span>Stop</span>
            </button>
          </div>

          {!effectiveIsGraphValid && (
            <p className="text-[11px] text-rose-400 flex items-center gap-1.5 font-medium">
              <AlertCircle className="w-3.5 h-3.5 shrink-0 text-rose-400" />
              <span>Generation disabled: resolve diagram errors to continue</span>
            </p>
          )}
          {/* Token Usage Stats */}
          {tokenUsage && (
            <div
              data-testid="token-usage"
              className="flex items-center justify-between text-[11px] text-[var(--vscode-text-muted,#8b949e)] bg-[var(--vscode-subcard-bg,#0d1117)] border border-[var(--vscode-border,#30363d)] rounded-md px-2 py-1.5 font-mono"
            >
              <span className="text-[var(--vscode-text-muted,#8b949e)] font-sans font-medium">Tokens:</span>
              <div className="flex items-center gap-2">
                <span>Input: <strong className="text-emerald-400 font-semibold">{tokenUsage.inputTokens.toLocaleString()}</strong></span>
                <span className="text-slate-600">•</span>
                <span>Output: <strong className="text-blue-400 font-semibold">{tokenUsage.outputTokens.toLocaleString()}</strong></span>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
