/**
 * @fileoverview Reducer and update helpers for managing pipeline configuration state.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { PipelineParadigm } from '../utils/sharedTypes';

/**
 * Language values accepted by `setLanguage` actions, including `'PySpark'` normalized to `'Python'`.
 */
export type PipelineLanguageInput = 'PySpark' | 'Python' | 'Java';

/**
 * Configuration settings for the pipeline generator, language target, and authoring paradigm.
 */
export interface PipelineConfig {
  language: 'Python' | 'Java';
  outputFolder: string;
  needsRebuild: boolean;
  paradigm?: PipelineParadigm;
}

/**
 * Discriminated union of actions supported by `pipelineConfigReducer`.
 */
export type PipelineConfigAction =
  | { type: 'setLanguage'; language: PipelineLanguageInput }
  | { type: 'setOutputFolder'; folder: string }
  | { type: 'setParadigm'; paradigm: PipelineParadigm }
  /** Host-driven sync when first hydrating from empty without dirtying the pipeline. */
  | { type: 'syncOutputFolderFromHost'; folder: string }
  | { type: 'markBuilt' }
  | { type: 'markDirty' }
  | { type: 'reset'; config: Omit<PipelineConfig, 'language'> & { language: PipelineLanguageInput } };

/**
 * Normalizes `'PySpark'` to `'Python'` so downstream file-lookup slices remain type-compatible.
 *
 * @param language Input language identifier.
 * @returns Normalized `'Python' | 'Java'` language value.
 */
export function normalizePipelineLanguage(language: PipelineLanguageInput): 'Python' | 'Java' {
  return language === 'Java' ? 'Java' : 'Python';
}

/**
 * Normalizes a pipeline paradigm identifier to `'imperative'` or `'sdp'`.
 *
 * @param paradigm Candidate paradigm identifier.
 * @returns Normalized `'imperative' | 'sdp'` value.
 */
export function normalizePipelineParadigm(paradigm: unknown): PipelineParadigm {
  return paradigm === 'sdp' ? 'sdp' : 'imperative';
}

/**
 * Pure helper that merges a partial patch (excluding `needsRebuild`) into `prev`
 * and flags `needsRebuild: true` if and only if any patched field differs from `prev`.
 * If no fields change, the original `prev` reference is preserved.
 *
 * @param prev The current pipeline configuration state.
 * @param patch Partial fields to compare and merge into the configuration.
 * @returns An updated PipelineConfig with needsRebuild: true if changed, or prev unchanged.
 */
export function withRebuildIfChanged(
  prev: PipelineConfig,
  patch: Partial<Omit<PipelineConfig, 'needsRebuild'>>
): PipelineConfig {
  const keys = Object.keys(patch) as (keyof Omit<PipelineConfig, 'needsRebuild'>)[];
  let changed = false;
  for (const key of keys) {
    if (patch[key] !== undefined && patch[key] !== prev[key]) {
      changed = true;
      break;
    }
  }

  if (!changed) {
    return prev;
  }

  return {
    ...prev,
    ...patch,
    needsRebuild: true,
  };
}

/**
 * Pure reducer function for pipeline configuration state transitions.
 * Guarantees referential equality when an action results in no effective state changes.
 *
 * @param state The current pipeline configuration state.
 * @param action The configuration action to process.
 * @returns The next pipeline configuration state or the identical state reference if no change occurred.
 */
export function pipelineConfigReducer(
  state: PipelineConfig,
  action: PipelineConfigAction
): PipelineConfig {
  switch (action.type) {
    case 'setLanguage':
      return withRebuildIfChanged(state, {
        language: normalizePipelineLanguage(action.language),
      });

    case 'setOutputFolder':
      return withRebuildIfChanged(state, { outputFolder: action.folder });

    case 'setParadigm': {
      const nextParadigm = normalizePipelineParadigm(action.paradigm);
      const currentParadigm = normalizePipelineParadigm(state.paradigm);
      if (
        currentParadigm === nextParadigm &&
        (state.paradigm === nextParadigm || nextParadigm === 'imperative')
      ) {
        return state;
      }
      return withRebuildIfChanged(state, { paradigm: nextParadigm });
    }

    case 'syncOutputFolderFromHost': {
      if (state.outputFolder === action.folder) {
        return state;
      }
      const needsRebuild =
        state.outputFolder !== '' && state.outputFolder !== action.folder
          ? true
          : state.needsRebuild;
      return {
        ...state,
        outputFolder: action.folder,
        needsRebuild,
      };
    }

    case 'markBuilt':
      if (!state.needsRebuild) {
        return state;
      }
      return {
        ...state,
        needsRebuild: false,
      };

    case 'markDirty':
      if (state.needsRebuild) {
        return state;
      }
      return {
        ...state,
        needsRebuild: true,
      };

    case 'reset': {
      const normalizedLanguage = normalizePipelineLanguage(action.config.language);
      if (
        state.language === normalizedLanguage &&
        state.outputFolder === action.config.outputFolder &&
        state.needsRebuild === action.config.needsRebuild &&
        state.paradigm === action.config.paradigm
      ) {
        return state;
      }
      return {
        ...action.config,
        language: normalizedLanguage,
      };
    }

    default:
      return state;
  }
}
