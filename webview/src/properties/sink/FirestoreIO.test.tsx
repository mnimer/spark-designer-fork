/**
 * @fileoverview Unit Tests for FirestoreIOSinkProperties Component (SNK30).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates FirestoreIOSinkProperties rendering, projectId and collectionPath validation,
 * WriteBatch operation switching ('set_merge' | 'set' | 'update' | 'delete'),
 * documentIdColumn requirement, 500-mutation WriteBatch warning, SecretManagerAuthFields,
 * and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { FirestoreIOSinkProperties, FirestoreSinkProperties } from './FirestoreIO';

describe('FirestoreIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports FirestoreSinkProperties alias and validates required projectId and collectionPath', () => {
    expect(FirestoreSinkProperties).toBe(FirestoreIOSinkProperties);
    render(
      <FirestoreIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('Project ID is required')).toBeInTheDocument();
    expect(screen.getByText('Collection path is required')).toBeInTheDocument();
    expect(screen.getByDisplayValue('(default)')).toBeInTheDocument();
    expect(screen.getByDisplayValue('500')).toBeInTheDocument();
  });

  it('hydrates Firestore WriteBatch properties in set_merge mode', () => {
    const node = {
      data: {
        properties: {
          projectId: 'analytics-gcp',
          databaseId: 'prod-db',
          collectionPath: 'orgs/acme/events',
          operation: 'set_merge' as const,
          documentIdColumn: 'event_id',
          mergeFields: 'status, score',
          maxBatchSize: 400,
          authMode: 'secretManager',
          secretName: 'firestore-sa-json',
        },
      },
    };

    const { container } = render(
      <FirestoreIOSinkProperties node={node} onChange={mockOnChange} />
    );

    expect(screen.getByDisplayValue('analytics-gcp')).toBeInTheDocument();
    expect(screen.getByDisplayValue('prod-db')).toBeInTheDocument();
    expect(screen.getByDisplayValue('orgs/acme/events')).toBeInTheDocument();
    expect(container.querySelector('#firestoreSinkOperation')).toHaveValue('set_merge');
    expect(screen.getByDisplayValue('event_id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('status, score')).toBeInTheDocument();
    expect(screen.getByDisplayValue('400')).toBeInTheDocument();
    expect(screen.getByDisplayValue('firestore-sa-json')).toBeInTheDocument();
  });

  it('requires documentIdColumn in delete and update modes and hides mergeFields', () => {
    const { container, rerender } = render(
      <FirestoreIOSinkProperties
        node={{
          data: {
            properties: {
              projectId: 'analytics-gcp',
              collectionPath: 'users',
              operation: 'set_merge',
              mergeFields: 'tier',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#firestoreSinkMergeFields')).toBeInTheDocument();

    const opSelect = container.querySelector('#firestoreSinkOperation') as HTMLSelectElement;
    fireEvent.change(opSelect, { target: { name: 'operation', value: 'delete' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ operation: 'delete' }));

    rerender(
      <FirestoreIOSinkProperties
        node={{
          data: {
            properties: {
              projectId: 'analytics-gcp',
              collectionPath: 'users',
              operation: 'delete',
              documentIdColumn: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#firestoreSinkMergeFields')).not.toBeInTheDocument();
    expect(
      screen.getByText('Document ID column is required for update or delete')
    ).toBeInTheDocument();

    const docIdInput = container.querySelector('#firestoreSinkDocumentIdColumn') as HTMLInputElement;
    fireEvent.change(docIdInput, { target: { name: 'documentIdColumn', value: 'user_id' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ documentIdColumn: 'user_id' })
    );
  });

  it('warns when maxBatchSize exceeds 500 mutations and updates system_instructions', () => {
    const { container } = render(
      <FirestoreIOSinkProperties
        node={{
          data: {
            properties: {
              projectId: 'analytics-gcp',
              collectionPath: 'users',
              maxBatchSize: 750,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText('Firestore caps each WriteBatch commit at 500 mutations')
    ).toBeInTheDocument();

    const textarea = container.querySelector('#firestoreSinkInstructions') as HTMLTextAreaElement;
    fireEvent.change(textarea, { target: { value: 'Add server_timestamp updated_at' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Add server_timestamp updated_at' })
    );
  });
});
