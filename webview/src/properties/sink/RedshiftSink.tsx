/**
 * Amazon Redshift Sink Properties Component (SNK26)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Amazon Redshift Sink nodes in Apache Spark 4.x
 * pipelines (.write.format("io.github.spark_redshift_community.spark.redshift") and foreachBatch).
 *
 * Architectural Decisions:
 * - Configures Redshift JDBC url, S3 staging tempdir (s3a://), tempformat (AVRO or CSV),
 *   destination dbtable, Spark saveMode, and physical DDL options (diststyle, distkey,
 *   sortkeyspec, preactions, and postactions).
 * - Supports AWS IAM Role assumption (aws_iam_role), S3A credential forwarding, and
 *   SecretManagerAuthFields for Redshift and S3 credentials.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type RedshiftSaveMode = 'append' | 'overwrite' | 'errorifexists' | 'ignore';
export type RedshiftTempFormat = 'AVRO' | 'CSV' | 'CSV GZIP';
export type RedshiftDistStyle = 'AUTO' | 'EVEN' | 'KEY' | 'ALL';

/** Props for the {@link RedshiftSinkProperties} component. */
export interface RedshiftSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        url?: string;
        tempdir?: string;
        tempformat?: RedshiftTempFormat;
        dbtable?: string;
        table?: string;
        saveMode?: RedshiftSaveMode;
        diststyle?: RedshiftDistStyle;
        distkey?: string;
        sortkeyspec?: string;
        preactions?: string;
        postactions?: string;
        awsIamRole?: string;
        forwardSparkS3Credentials?: boolean;
        username?: string;
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
 * Configures properties for Apache Spark Amazon Redshift sink nodes.
 */
export const RedshiftSinkProperties: React.FC<RedshiftSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const url = properties.url || '';
  const tempdir = properties.tempdir || '';
  const tempformat: RedshiftTempFormat = properties.tempformat || 'AVRO';
  const dbtable = properties.dbtable ?? properties.table ?? '';
  const saveMode: RedshiftSaveMode = properties.saveMode || 'append';
  const diststyle: RedshiftDistStyle = properties.diststyle || 'AUTO';
  const distkey = properties.distkey || '';
  const sortkeyspec = properties.sortkeyspec || '';
  const preactions = properties.preactions || '';
  const postactions = properties.postactions || '';
  const awsIamRole = properties.awsIamRole || '';
  const forwardSparkS3Credentials = properties.forwardSparkS3Credentials ?? false;
  const username = properties.username || '';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';

  const isUrlEmpty = !url.trim();
  const isTempdirEmpty = !tempdir.trim();
  const isTableEmpty = !String(dbtable).trim();
  const isDistKeyEmpty = diststyle === 'KEY' && !distkey.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    delete next.password;
    if (name === 'dbtable') {
      delete next.table;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark Amazon Redshift Sink</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          spark.redshift · foreachBatch
        </span>
      </div>

      {/* Redshift JDBC URL & S3 Staging Directory */}
      <PropertyField
        id="redshiftSinkUrl"
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

      <div className="grid grid-cols-3 gap-2">
        <div className="col-span-2">
          <PropertyField
            id="redshiftSinkTempdir"
            name="tempdir"
            label="S3 Staging Temp Directory (tempdir)"
            required="Temporary S3 staging directory is required"
            tooltip="S3 URI (s3a://) where Spark stages partition files before Redshift runs parallel COPY."
            value={tempdir}
            invalid={isTempdirEmpty}
            error={isTempdirEmpty ? 'Temporary S3 staging directory is required' : null}
            placeholder="s3a://my-redshift-staging-bucket/spark-temp/"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <div className="flex flex-col gap-2">
          <label
            htmlFor="redshiftSinkTempformat"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Temp Format</span>
            <InfoTooltip text="Intermediate file format written to tempdir for Redshift COPY (AVRO recommended)." />
          </label>
          <select
            id="redshiftSinkTempformat"
            name="tempformat"
            value={tempformat}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="AVRO">AVRO</option>
            <option value="CSV">CSV</option>
            <option value="CSV GZIP">CSV GZIP</option>
          </select>
        </div>
      </div>

      {/* Destination Table, Save Mode & Physical Layout */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <label className="flex items-center text-xs font-bold uppercase text-slate-500">
          <span>Target Table, Layout &amp; SQL Actions</span>
          <InfoTooltip text="Configures destination dbtable, Spark saveMode, Redshift distribution/sort keys, and SQL hooks." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="redshiftSinkDbtable"
            name="dbtable"
            label="Destination Table (dbtable)"
            required="dbtable is required"
            tooltip="Target Redshift schema and table name (for example public.sales_summary)."
            value={dbtable}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'dbtable is required' : null}
            placeholder="public.sales_summary"
            onChange={handleChange}
            mono={true}
          />

          <div className="flex flex-col gap-2">
            <label
              htmlFor="redshiftSinkSaveMode"
              className="flex items-center text-xs font-bold uppercase text-slate-500"
            >
              <span>Save Mode</span>
              <InfoTooltip text="Spark save mode applied when loading data into Redshift." />
            </label>
            <select
              id="redshiftSinkSaveMode"
              name="saveMode"
              value={saveMode}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="append">append</option>
              <option value="overwrite">overwrite</option>
              <option value="errorifexists">errorifexists</option>
              <option value="ignore">ignore</option>
            </select>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-2">
            <label
              htmlFor="redshiftSinkDiststyle"
              className="flex items-center text-xs font-bold uppercase text-slate-500"
            >
              <span>Distribution Style (diststyle)</span>
              <InfoTooltip text="Redshift table distribution style applied when creating new tables." />
            </label>
            <select
              id="redshiftSinkDiststyle"
              name="diststyle"
              value={diststyle}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="AUTO">AUTO</option>
              <option value="EVEN">EVEN</option>
              <option value="KEY">KEY</option>
              <option value="ALL">ALL</option>
            </select>
          </div>

          <PropertyField
            id="redshiftSinkSortkeyspec"
            name="sortkeyspec"
            label="Sort Key Spec (sortkeyspec)"
            tooltip="Optional Redshift SORTKEY clause applied when creating tables."
            value={sortkeyspec}
            placeholder="SORTKEY(sale_date, region)"
            onChange={handleChange}
            mono={true}
          />
        </div>

        {diststyle === 'KEY' && (
          <PropertyField
            id="redshiftSinkDistkey"
            name="distkey"
            label="Distribution Key Column (distkey)"
            required="distkey is required when diststyle is KEY"
            tooltip="Column name used for Redshift KEY distribution."
            value={distkey}
            invalid={isDistKeyEmpty}
            error={isDistKeyEmpty ? 'distkey is required when diststyle is KEY' : null}
            placeholder="customer_id"
            onChange={handleChange}
            mono={true}
          />
        )}

        <PropertyField
          id="redshiftSinkPreactions"
          name="preactions"
          label="Pre-Actions SQL (preactions)"
          type="textarea"
          rows={2}
          tooltip="Optional SQL commands executed on Redshift before COPY runs."
          value={preactions}
          placeholder="DELETE FROM public.sales_summary WHERE sale_date = CURRENT_DATE"
          onChange={handleChange}
          mono={true}
        />

        <PropertyField
          id="redshiftSinkPostactions"
          name="postactions"
          label="Post-Actions SQL (postactions)"
          type="textarea"
          rows={2}
          tooltip="Optional SQL commands executed on Redshift after COPY completes."
          value={postactions}
          placeholder="GRANT SELECT ON public.sales_summary TO GROUP bi_users"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* IAM Role, S3 Credential Forwarding & Redshift User */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <PropertyField
          id="redshiftSinkAwsIamRole"
          name="awsIamRole"
          label="AWS IAM Role ARN (aws_iam_role)"
          tooltip="IAM role ARN assumed by Redshift COPY to read staged files from S3."
          value={awsIamRole}
          placeholder="arn:aws:iam::123456789012:role/MyRedshiftS3Role"
          onChange={handleChange}
          mono={true}
        />

        <div className="flex items-center gap-2">
          <input
            id="redshiftSinkForwardSparkS3Credentials"
            type="checkbox"
            name="forwardSparkS3Credentials"
            checked={forwardSparkS3Credentials}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="redshiftSinkForwardSparkS3Credentials"
            className="flex items-center text-xs text-slate-300 cursor-pointer"
          >
            <span>Forward Spark S3 Credentials (forward_spark_s3_credentials)</span>
            <InfoTooltip text="Forwards temporary or Hadoop S3A credentials from Spark to Redshift when aws_iam_role is not used." />
          </label>
        </div>

        <PropertyField
          id="redshiftSinkUsername"
          name="username"
          label="Redshift Username"
          tooltip="Redshift database user name."
          value={username}
          placeholder="rs_writer"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Secret Manager Authentication */}
      <SecretManagerAuthFields
        idPrefix="redshiftSink"
        serviceName="Amazon Redshift"
        authMode={authMode}
        secretName={secretName}
        datalistId="redshift-sink-secret-options"
        secretPlaceholder="e.g. redshift-credentials-secret"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="redshiftSinkSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Amazon Redshift sink code generation..."
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
