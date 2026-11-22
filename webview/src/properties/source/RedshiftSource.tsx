/**
 * Amazon Redshift Source Properties Component (SRC29)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Amazon Redshift Source nodes
 * in Apache Spark 4.x pipelines (.format("io.github.spark_redshift_community.spark.redshift")).
 *
 * Architectural Decisions:
 * - Batch-only source reading from Redshift via S3/GCS temporary staging directory (tempdir).
 * - Configures JDBC url, dbtable or query, tempdir, awsIamRole, forwardSparkS3Credentials toggle,
 *   and SecretManagerAuthFields.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Props for the {@link RedshiftSourceProperties} component. */
export interface RedshiftSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        url?: string;
        readTargetMode?: 'dbtable' | 'query';
        dbtable?: string;
        query?: string;
        tempdir?: string;
        awsIamRole?: string;
        forwardSparkS3Credentials?: boolean;
        authMode?: string;
        secretName?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Configures properties for Apache Spark Amazon Redshift batch source nodes.
 */
export const RedshiftSourceProperties: React.FC<RedshiftSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const url = properties.url || '';
  const dbtable = properties.dbtable || '';
  const query = properties.query || '';
  const readTargetMode: 'dbtable' | 'query' =
    properties.readTargetMode || (query.trim() && !dbtable.trim() ? 'query' : 'dbtable');
  const tempdir = properties.tempdir || '';
  const awsIamRole = properties.awsIamRole || '';
  const forwardSparkS3Credentials = properties.forwardSparkS3Credentials ?? false;
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';

  const isUrlEmpty = !url.trim();
  const isTempdirEmpty = !tempdir.trim();
  const isTableEmpty = readTargetMode === 'dbtable' && !dbtable.trim();
  const isQueryEmpty = readTargetMode === 'query' && !query.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    onChange({ ...properties, [name]: updatedValue });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark Amazon Redshift Connector</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          spark.redshift · BATCH
        </span>
      </div>

      {/* Redshift JDBC URL & Staging Directory */}
      <PropertyField
        id="redshiftSourceUrl"
        name="url"
        label="Redshift JDBC URL (url)"
        required="Redshift JDBC URL is required"
        tooltip="Redshift JDBC connection URL (jdbc:redshift://<endpoint>:5439/<database>)."
        value={url}
        invalid={isUrlEmpty}
        error={isUrlEmpty ? 'Redshift JDBC URL is required' : null}
        placeholder="jdbc:redshift://my-cluster.abc123xyz.us-east-1.redshift.amazonaws.com:5439/dev"
        onChange={handleChange}
        mono={true}
      />

      <PropertyField
        id="redshiftSourceTempdir"
        name="tempdir"
        label="Staging Temp Directory (tempdir)"
        required="Temporary S3/GCS staging directory is required"
        tooltip="Cloud storage URI (s3a:// or gs://) used by UNLOAD for high-throughput parallel data transfer."
        value={tempdir}
        invalid={isTempdirEmpty}
        error={isTempdirEmpty ? 'Temporary S3/GCS staging directory is required' : null}
        placeholder="s3a://my-redshift-staging-bucket/spark-temp/"
        onChange={handleChange}
        mono={true}
      />

      {/* Read Target (dbtable vs query) */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="redshiftSourceReadTargetMode"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Read Target</span>
            <InfoTooltip text="Select whether to read a Redshift table (dbtable) or execute a custom SQL query." />
          </label>
          <select
            id="redshiftSourceReadTargetMode"
            name="readTargetMode"
            value={readTargetMode}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="dbtable">Table (.option(&quot;dbtable&quot;, ...))</option>
            <option value="query">SQL Query (.option(&quot;query&quot;, ...))</option>
          </select>
        </div>

        {readTargetMode === 'dbtable' ? (
          <PropertyField
            id="redshiftSourceDbtable"
            name="dbtable"
            label="Table Name (dbtable)"
            required="dbtable is required in Table mode"
            tooltip="Redshift schema and table name (for example public.sales)."
            value={dbtable}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'dbtable is required in Table mode' : null}
            placeholder="public.sales"
            onChange={handleChange}
            mono={true}
          />
        ) : (
          <PropertyField
            id="redshiftSourceQuery"
            name="query"
            label="SQL Query (query)"
            type="textarea"
            rows={3}
            required="SQL query is required in Query mode"
            tooltip="SQL SELECT query unloaded by Redshift into tempdir."
            value={query}
            invalid={isQueryEmpty}
            error={isQueryEmpty ? 'SQL query is required in Query mode' : null}
            onChange={handleChange}
            placeholder="SELECT sale_id, buyer_id, pricepaid FROM public.sales WHERE saletime >= '2026-01-01'"
            mono={true}
          />
        )}
      </div>

      {/* IAM Role & S3 Credential Forwarding */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <PropertyField
          id="redshiftSourceAwsIamRole"
          name="awsIamRole"
          label="AWS IAM Role ARN (aws_iam_role)"
          tooltip="IAM role ARN assumed by Redshift UNLOAD to access the S3 staging bucket."
          value={awsIamRole}
          placeholder="arn:aws:iam::123456789012:role/MyRedshiftS3Role"
          onChange={handleChange}
          mono={true}
        />

        <div className="flex items-center gap-2">
          <input
            id="redshiftSourceForwardSparkS3Credentials"
            type="checkbox"
            name="forwardSparkS3Credentials"
            checked={forwardSparkS3Credentials}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="redshiftSourceForwardSparkS3Credentials"
            className="flex items-center text-xs text-slate-300 cursor-pointer"
          >
            <span>Forward Spark S3 Credentials (forward_spark_s3_credentials)</span>
            <InfoTooltip text="Forwards the Spark Hadoop S3A credentials to Redshift when aws_iam_role is not used." />
          </label>
        </div>
      </div>

      {/* Secret Manager Authentication */}
      <SecretManagerAuthFields
        idPrefix="redshiftSource"
        serviceName="Amazon Redshift"
        authMode={authMode}
        secretName={secretName}
        datalistId="redshift-source-secret-options"
        secretPlaceholder="e.g. redshift-credentials-secret"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="redshiftSourceSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Amazon Redshift source code generation..."
        onChange={(value) =>
          onChange({
            ...properties,
            system_instructions: value,
          })
        }
      />
    </div>
  );
};
