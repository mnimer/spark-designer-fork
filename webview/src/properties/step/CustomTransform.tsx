/**
 * @fileoverview Property configuration panel for the Apache Spark Custom Transform step (CustomTransform).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React from 'react';
import { AlertTriangle, Sparkles } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Persisted properties for the CustomTransform step node.
 */
export interface CustomTransformStepProperties {
  functionName?: string;
  outputSchemaHint?: string;
  preserveExistingColumns?: boolean;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link CustomTransformProperties}.
 */
export interface CustomTransformNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: CustomTransformStepProperties;
  };
}

/**
 * Props for the {@link CustomTransformProperties} component.
 */
export interface CustomTransformPropertiesProps {
  node: CustomTransformNode;
  onChange: (properties: CustomTransformStepProperties) => void;
}

const VALID_PYTHON_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Renders the property configuration panel for the Apache Spark CustomTransform step.
 *
 * @param props Selected CustomTransform node and property update callback.
 * @returns The rendered CustomTransform configuration form.
 */
export const CustomTransformProperties: React.FC<CustomTransformPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: CustomTransformStepProperties = node?.data?.properties || {};
  const functionName =
    typeof properties.functionName === 'string'
      ? properties.functionName
      : 'custom_transform';
  const outputSchemaHint =
    typeof properties.outputSchemaHint === 'string' ? properties.outputSchemaHint : '';
  const preserveExistingColumns =
    typeof properties.preserveExistingColumns === 'boolean'
      ? properties.preserveExistingColumns
      : true;
  const systemInstructions = resolveSystemInstructions(node);

  const isFunctionNameInvalid =
    functionName.trim().length === 0 || !VALID_PYTHON_IDENT.test(functionName.trim());

  const handleFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    onChange({
      ...properties,
      functionName,
      preserveExistingColumns,
      [name]: value,
    });
  };

  const handlePreserveToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange({
      ...properties,
      functionName,
      preserveExistingColumns: e.target.checked,
    });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      functionName,
      preserveExistingColumns,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Catalyst & Spark Connect Guidance Banner */}
      <div
        data-testid="custom-transform-catalyst-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <Sparkles className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Modular df.transform() Step</span>
          <InfoTooltip text="Generates a pure DataFrame helper invoked via df.transform(fn) using native Catalyst pyspark.sql.functions without RDD or SparkContext calls." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          Describe your transformation logic in the prompt block below. Generated code wraps native
          Catalyst <code className="font-mono text-[var(--vscode-text-primary)]">pyspark.sql.functions</code> inside{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">df.transform()</code> for full Spark Connect
          compatibility.
        </p>
      </div>

      {/* 2. Function Name & Schema Hint */}
      <PropertyField
        id="customTransformFunctionName"
        name="functionName"
        label="Helper Function Name"
        required="Required"
        tooltip="Python function identifier generated and passed to df.transform(functionName)."
        value={functionName}
        onChange={handleFieldChange}
        placeholder="custom_transform"
        invalid={isFunctionNameInvalid}
        error={
          isFunctionNameInvalid
            ? 'Function name must be a valid Python identifier.'
            : null
        }
      />

      <PropertyField
        id="customTransformOutputSchemaHint"
        name="outputSchemaHint"
        label="Output Schema Hint (Optional DDL)"
        tooltip="Optional Spark DDL string describing expected output columns and types, such as user_id string, net_revenue double."
        value={outputSchemaHint}
        onChange={handleFieldChange}
        placeholder="e.g. user_id string, net_revenue double, is_vip boolean"
        helperText="Leave blank to let Catalyst infer output column types automatically."
      />

      {/* 3. Preserve Existing Columns Toggle */}
      <div className="flex items-center justify-between gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <label
          htmlFor="customTransformPreserveExisting"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)] cursor-pointer"
        >
          <span>Preserve Existing Columns</span>
          <InfoTooltip text="When enabled, uses df.withColumns({...}) to retain upstream columns alongside derived fields. When disabled, projects only output columns via df.select(...)." />
        </label>
        <input
          id="customTransformPreserveExisting"
          name="preserveExistingColumns"
          type="checkbox"
          checked={preserveExistingColumns}
          onChange={handlePreserveToggle}
          className="w-4 h-4 accent-blue-500 cursor-pointer"
        />
      </div>

      {/* 4. Dead-Letter Queue Guidance Banner */}
      <div
        data-testid="custom-transform-dlq-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
          <span>Dead-Letter Queue (error Handle)</span>
          <InfoTooltip text="When the error handle is connected, fallible conversions populate _error via try_cast or vectorized try/except so invalid rows route to DLQ." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          If you wire the <code className="font-mono text-[var(--vscode-text-primary)]">error</code> handle,
          fallible operations capture diagnostic messages into{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">_error</code> and split invalid rows to the
          dead-letter branch.
        </p>
      </div>

      <SystemInstructionsField
        id="customTransformSystemInstructions"
        label="Custom Transformation Prompt & Instructions"
        rows={6}
        value={systemInstructions}
        tooltip="Primary specification telling the code generator what Catalyst DataFrame operations to synthesize inside df.transform()."
        placeholder="Describe the exact columns, formulas, business rules, or conditional expressions to compute..."
        helperText="Prefer declarative column rules so Spark compiles them into native Catalyst expressions."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
