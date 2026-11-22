/**
 * @fileoverview Unit tests for the FeaturePrep step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { FeaturePrepProperties } from './FeaturePrep';

describe('FeaturePrepProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-feature-prep-1',
    data: {
      label: 'Feature Prep',
      spark_transform: 'FeaturePrep',
      properties,
    },
  });

  it('renders defaults and warns when no columns or pretrainedPipelineUri are configured', () => {
    const handleChange = vi.fn();
    render(<FeaturePrepProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByLabelText(/Enable One-Hot Encoding/i, { selector: 'input' })).toBeChecked();
    expect(
      screen.getByLabelText(/Numeric Imputation Strategy/i, { selector: 'select' })
    ).toHaveValue('none');
    expect(screen.getByLabelText(/Feature Scaler/i, { selector: 'select' })).toHaveValue('none');
    expect(
      screen.getByLabelText(/Invalid \/ Unseen Value Policy/i, { selector: 'select' })
    ).toHaveValue('keep');
    expect(screen.getByLabelText(/Output Features Column/i, { selector: 'input' })).toHaveValue(
      'features'
    );
    expect(
      screen.getByText(/Configure at least one categorical or numeric column/i)
    ).toBeInTheDocument();
  });

  it('adds and removes categoricalColumns and numericColumns and toggles enableOneHotEncoding', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <FeaturePrepProperties node={createNode()} onChange={handleChange} />
    );

    fireEvent.change(screen.getByLabelText(/Categorical Columns/i, { selector: 'input' }), {
      target: { value: 'region, device' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Add categorical column' }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        categoricalColumns: ['region', 'device'],
      })
    );

    handleChange.mockClear();
    rerender(
      <FeaturePrepProperties
        node={createNode({ categoricalColumns: ['region', 'device'] })}
        onChange={handleChange}
      />
    );

    // Warning disappears once columns are configured
    expect(
      screen.queryByText(/Configure at least one categorical or numeric column/i)
    ).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Numeric Columns/i, { selector: 'input' }), {
      target: { value: 'age, spend' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Add numeric column' }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        categoricalColumns: ['region', 'device'],
        numericColumns: ['age', 'spend'],
      })
    );

    handleChange.mockClear();
    rerender(
      <FeaturePrepProperties
        node={createNode({
          categoricalColumns: ['region', 'device'],
          numericColumns: ['age', 'spend'],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove categorical column region' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        categoricalColumns: ['device'],
      })
    );

    handleChange.mockClear();
    fireEvent.click(screen.getByLabelText(/Enable One-Hot Encoding/i, { selector: 'input' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        enableOneHotEncoding: false,
      })
    );
  });

  it('updates imputeStrategy, scalerType, handleInvalid, outputFeaturesColumn, pretrainedPipelineUri, and instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <FeaturePrepProperties
        node={createNode({ numericColumns: ['spend'] })}
        onChange={handleChange}
      />
    );

    fireEvent.change(
      screen.getByLabelText(/Numeric Imputation Strategy/i, { selector: 'select' }),
      { target: { value: 'median' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({ imputeStrategy: 'median' })
    );

    fireEvent.change(screen.getByLabelText(/Feature Scaler/i, { selector: 'select' }), {
      target: { value: 'standard' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({ scalerType: 'standard' })
    );

    fireEvent.change(
      screen.getByLabelText(/Invalid \/ Unseen Value Policy/i, { selector: 'select' }),
      { target: { value: 'skip' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({ handleInvalid: 'skip' })
    );

    fireEvent.change(
      screen.getByLabelText(/Pre-Trained PipelineModel URI/i, { selector: 'input' }),
      { target: { value: 'gs://ml-bucket/models/prep-v2' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({ pretrainedPipelineUri: 'gs://ml-bucket/models/prep-v2' })
    );

    // Empty outputFeaturesColumn triggers required validation
    rerender(
      <FeaturePrepProperties
        node={createNode({ numericColumns: ['spend'], outputFeaturesColumn: '' })}
        onChange={handleChange}
      />
    );
    expect(screen.getByText('Output features column is required.')).toBeInTheDocument();

    fireEvent.change(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }),
      { target: { value: 'Cast boolean flags to double before VectorAssembler' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Cast boolean flags to double before VectorAssembler',
      })
    );
  });
});
