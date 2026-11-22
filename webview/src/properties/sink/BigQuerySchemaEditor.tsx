/**
 * BigQuery Schema Editor Component (Re-export from shared SchemaEditor)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Re-exports the multi-tab SchemaEditor and its pure conversion utilities
 * for backwards compatibility with BigQueryIO and existing tests.
 */

export {
  SchemaEditor as BigQuerySchemaEditor,
  type SchemaField as BigQueryField,
  type SchemaEditorProps as BigQuerySchemaEditorProps,
} from '../../components/common/SchemaEditor';
