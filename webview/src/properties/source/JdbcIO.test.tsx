/**
 * Unit Tests for JdbcIOSourceProperties Component (SRC27)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates JdbcIOSourceProperties rendering, dbEngine presets, jdbcUrl/driverClass,
 * table vs query mode, dynamicBounds toggle vs lowerBound/upperBound, fetchsize,
 * driverJarUri, SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  JdbcIOSourceProperties,
  JdbcSourceProperties,
  getJdbcUrlExample,
  JDBC_DRIVER_PRESETS,
} from './JdbcIO';

describe('JdbcIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports JdbcSourceProperties alias and computes JDBC URL examples across presets', () => {
    expect(JdbcSourceProperties).toBe(JdbcIOSourceProperties);
    expect(getJdbcUrlExample('org.postgresql.Driver')).toBe('jdbc:postgresql://localhost:5432/mydb');
    expect(getJdbcUrlExample('com.mysql.cj.jdbc.Driver')).toBe('jdbc:mysql://localhost:3306/mydb');
    expect(getJdbcUrlExample('com.microsoft.sqlserver.jdbc.SQLServerDriver')).toBe(
      'jdbc:sqlserver://localhost:1433;databaseName=mydb'
    );
    expect(getJdbcUrlExample('oracle.jdbc.OracleDriver')).toBe(
      'jdbc:oracle:thin:@//localhost:1521/ORCLPDB1'
    );
    expect(getJdbcUrlExample('com.ibm.db2.jcc.DB2Driver')).toBe('jdbc:db2://localhost:50000/mydb');
    expect(getJdbcUrlExample('custom')).toBe('jdbc:<driver>://<host>:<port>/<database>');
    expect(JDBC_DRIVER_PRESETS.length).toBe(6);
  });

  it('renders dbEngine preset, auto-updates driverClass, and handles table/query switching', () => {
    const node = {
      data: {
        properties: {
          dbEngine: 'postgresql' as const,
          jdbcUrl: 'jdbc:postgresql://localhost:5432/testdb',
          table: 'public.orders',
          partitionColumn: 'id',
          numPartitions: 8,
          fetchsize: 5000,
        },
      },
    };

    const { container } = render(<JdbcIOSourceProperties node={node} onChange={mockOnChange} />);

    const engineSelect = container.querySelector('#jdbcSourceDbEngine') as HTMLSelectElement;
    expect(engineSelect).toHaveValue('postgresql');
    expect(container.querySelector('#jdbcDriverClass')).toHaveValue('org.postgresql.Driver');
    expect(container.querySelector('#jdbcUrl')).toHaveValue('jdbc:postgresql://localhost:5432/testdb');
    expect(container.querySelector('#jdbcTable')).toHaveValue('public.orders');

    fireEvent.change(engineSelect, { target: { name: 'dbEngine', value: 'mysql' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dbEngine: 'mysql',
        driverClass: 'com.mysql.cj.jdbc.Driver',
      })
    );
  });

  it('toggles dynamicBounds and renders manual lowerBound and upperBound when unchecked', () => {
    const { container, rerender } = render(
      <JdbcIOSourceProperties
        node={{
          data: {
            properties: {
              jdbcUrl: 'jdbc:postgresql://localhost:5432/testdb',
              table: 'orders',
              dynamicBounds: true,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const dynamicCheckbox = container.querySelector('#jdbcDynamicBounds') as HTMLInputElement;
    expect(dynamicCheckbox).toBeChecked();
    expect(container.querySelector('#jdbcLowerBound')).not.toBeInTheDocument();

    fireEvent.click(dynamicCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ dynamicBounds: false }));

    rerender(
      <JdbcIOSourceProperties
        node={{
          data: {
            properties: {
              jdbcUrl: 'jdbc:postgresql://localhost:5432/testdb',
              table: 'orders',
              dynamicBounds: false,
              lowerBound: 100,
              upperBound: 9000,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#jdbcLowerBound')).toHaveValue(100);
    expect(container.querySelector('#jdbcUpperBound')).toHaveValue(9000);
  });

  it('cleans up legacy plaintext password and updates driverJarUri and system_instructions', () => {
    const { container } = render(
      <JdbcIOSourceProperties
        node={{
          data: {
            properties: {
              jdbcUrl: 'jdbc:postgresql://localhost:5432/testdb',
              table: 'orders',
              password: 'legacy-plaintext-password',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('input[name="password"]')).not.toBeInTheDocument();

    const jarInput = container.querySelector('#jdbcDriverJarUri') as HTMLInputElement;
    fireEvent.change(jarInput, { target: { name: 'driverJarUri', value: 'gs://b/driver.jar' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ driverJarUri: 'gs://b/driver.jar' })
    );
    expect(mockOnChange.mock.calls[0][0].password).toBeUndefined();

    const sysInput = container.querySelector('#jdbcSourceSystemInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Partition by order_id' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Partition by order_id' })
    );
  });
});
