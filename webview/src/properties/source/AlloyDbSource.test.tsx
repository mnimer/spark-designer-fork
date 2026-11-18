/**
 * Unit Tests for AlloyDbSourceProperties Component (SRC26)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates AlloyDbSourceProperties rendering, alloydbInstanceUri format validation,
 * ipType selector, dynamicBounds vs manual bounds toggle, SecretManagerAuthFields,
 * Conscrypt removal note, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { AlloyDbSourceProperties } from './AlloyDbSource';

describe('AlloyDbSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders Conscrypt helper note and validates required fields when empty', () => {
    render(<AlloyDbSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText(/dataproc\.artifacts\.remove=conscrypt/i)).toBeInTheDocument();
    expect(screen.getByText('AlloyDB instance URI is required')).toBeInTheDocument();
    expect(screen.getByText('Database is required')).toBeInTheDocument();
    expect(screen.getByText('Table name is required in Table mode')).toBeInTheDocument();
  });

  it('validates malformed alloydbInstanceUri and accepts canonical projects/locations/clusters/instances URI', () => {
    const { rerender } = render(
      <AlloyDbSourceProperties
        node={{ data: { properties: { alloydbInstanceUri: 'invalid-uri', database: 'postgres', table: 't' } } }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText('Use projects/<project>/locations/<region>/clusters/<cluster>/instances/<instance>')
    ).toBeInTheDocument();

    rerender(
      <AlloyDbSourceProperties
        node={{
          data: {
            properties: {
              alloydbInstanceUri:
                'projects/my-proj/locations/us-central1/clusters/my-cluster/instances/my-inst',
              ipType: 'PSC',
              database: 'postgres',
              table: 'public.ledger',
              dynamicBounds: false,
              lowerBound: 10,
              upperBound: 25000,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.queryAllByRole('alert')).toHaveLength(0);
    expect(screen.getByDisplayValue('PSC (Private Service Connect)')).toBeInTheDocument();
    expect(screen.getByDisplayValue('10')).toBeInTheDocument();
    expect(screen.getByDisplayValue('25000')).toBeInTheDocument();
  });

  it('dispatches updates for ipType, dynamicBounds, JAR URIs, and system_instructions', () => {
    const { container } = render(
      <AlloyDbSourceProperties
        node={{
          data: {
            properties: {
              alloydbInstanceUri:
                'projects/my-proj/locations/us-central1/clusters/my-cluster/instances/my-inst',
              database: 'postgres',
              table: 'public.ledger',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const ipSelect = container.querySelector('#alloydbSourceIpType') as HTMLSelectElement;
    fireEvent.change(ipSelect, { target: { name: 'ipType', value: 'PUBLIC' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ ipType: 'PUBLIC' }));

    const dynamicCheckbox = container.querySelector('#alloydbSourceDynamicBounds') as HTMLInputElement;
    fireEvent.click(dynamicCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ dynamicBounds: false }));

    const driverInput = container.querySelector('#alloydbSourceDriverJarUri') as HTMLInputElement;
    fireEvent.change(driverInput, { target: { name: 'driverJarUri', value: 'gs://b/pg.jar' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ driverJarUri: 'gs://b/pg.jar' }));

    const sysInput = container.querySelector('#alloydbSourceSystemInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Use AlloyDB PSC endpoint' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Use AlloyDB PSC endpoint' })
    );
  });
});
