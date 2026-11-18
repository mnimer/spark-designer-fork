/**
 * SocketSource Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating SocketSourceProperties component functionality,
 * default host ('localhost') and port (9999), validation for empty host or invalid port,
 * includeTimestamp toggle, and system_instructions.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SocketSourceProperties } from './SocketSource';

describe('SocketSourceProperties', () => {
  it('renders default host=localhost, port=9999, and includeTimestamp=false', () => {
    const mockNode = {
      data: {
        label: 'Socket Source',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    render(<SocketSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByText('STREAMING ONLY (DEBUG)')).toBeInTheDocument();
    expect(screen.getByLabelText(/Socket Host/i)).toHaveValue('localhost');
    expect(screen.getByLabelText(/Socket Port/i)).toHaveValue(9999);
    expect(screen.getByLabelText(/Include Timestamp Column/i)).not.toBeChecked();

    fireEvent.change(screen.getByLabelText(/Socket Host/i), {
      target: { name: 'host', value: '127.0.0.1' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        readMode: 'streaming',
        host: '127.0.0.1',
      })
    );

    fireEvent.change(screen.getByLabelText(/Socket Port/i), {
      target: { name: 'port', value: '8085' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        port: '8085',
      })
    );

    fireEvent.click(screen.getByLabelText(/Include Timestamp Column/i));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        includeTimestamp: true,
      })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i), {
      target: { value: 'Tokenize socket lines by whitespace' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Tokenize socket lines by whitespace',
      })
    );
  });

  it('validates empty host and out-of-range port', () => {
    const mockOnChange = vi.fn();
    render(
      <SocketSourceProperties
        node={{
          data: {
            label: 'Socket Source',
            properties: {
              host: '',
              port: 70000,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText('Socket Host is required.')).toBeInTheDocument();
    expect(screen.getByText('Valid TCP port (1-65535) is required.')).toBeInTheDocument();
  });
});
