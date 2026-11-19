/**
 * @fileoverview Tests for the runtime-variable environment sanitizer (finding S1).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import { isAllowedEnvKey, applyRuntimeVariables } from '../../security/envSanitizer';

describe('isAllowedEnvKey', () => {
    it.each(['DB_HOST', 'my_var', '_PRIVATE', 'GCS_BUCKET2', 'Project_Id'])('accepts %s', (key) => {
        expect(isAllowedEnvKey(key)).toBe(true);
    });

    it.each([
        '', ' ', '1ABC', 'A-B', 'A B', 'A=B', 'A;rm', '$(id)', 'X\nY',
    ])('rejects malformed identifier %j', (key) => {
        expect(isAllowedEnvKey(key)).toBe(false);
    });

    it.each([
        'PATH', 'BASH_ENV', 'ENV', 'SHELLOPTS', 'BASHOPTS', 'CDPATH', 'IFS', 'PS4',
        'LD_PRELOAD', 'LD_LIBRARY_PATH', 'DYLD_INSERT_LIBRARIES',
        'PYTHONPATH', 'PYTHONSTARTUP', 'PYTHONHOME',
        'JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS', 'NODE_OPTIONS',
        'path', 'ld_preload',
    ])('rejects dangerous variable %s', (key) => {
        expect(isAllowedEnvKey(key)).toBe(false);
    });

    it('rejects non-string input', () => {
        expect(isAllowedEnvKey(undefined as unknown as string)).toBe(false);
        expect(isAllowedEnvKey(42 as unknown as string)).toBe(false);
    });
});

describe('applyRuntimeVariables', () => {
    it('copies allowed keys (trimmed) and reports rejected ones', () => {
        const env: Record<string, string> = { PATH: '/usr/bin' };
        const rejected = applyRuntimeVariables(env, [
            { key: ' DB_HOST ', value: 'db' },
            { key: 'PATH', value: '/evil' },
            { key: 'BASH_ENV', value: '/tmp/x.sh' },
            { key: 'EMPTY', value: undefined as unknown as string },
            { key: '   ', value: 'ignored' },
        ]);
        expect(env).toEqual({ PATH: '/usr/bin', DB_HOST: 'db', EMPTY: '' });
        expect(rejected).toEqual(['PATH', 'BASH_ENV']);
    });

    it('tolerates non-array input', () => {
        const env: Record<string, string> = {};
        expect(applyRuntimeVariables(env, undefined)).toEqual([]);
        expect(applyRuntimeVariables(env, 'nope' as unknown as [])).toEqual([]);
        expect(env).toEqual({});
    });
});

describe('Spark denylist keys (T5)', () => {
    const SPARK_DENIED_KEYS = [
        'PYSPARK_PYTHON',
        'PYSPARK_DRIVER_PYTHON',
        'SPARK_SUBMIT_OPTS',
        'SPARK_CONF_DIR',
        'SPARK_HOME',
        'SPARK_DIST_CLASSPATH',
    ];

    it.each(SPARK_DENIED_KEYS)('rejects Spark runtime control variable %s', (key) => {
        expect(isAllowedEnvKey(key)).toBe(false);
    });

    it.each(SPARK_DENIED_KEYS.flatMap((key) => [key.toLowerCase(), key[0] + key.slice(1).toLowerCase()]))(
        'rejects Spark denylist key %s in any letter case',
        (key) => {
            expect(isAllowedEnvKey(key)).toBe(false);
        }
    );

    it.each(['PYTHONINSPECT', 'MAVEN_OPTS', 'PROMPT_COMMAND', 'BASH_FUNC_x%%', 'BASH_FUNC_evil', 'DYLD_LIBRARY_PATH', 'LD_'])(
        'rejects other interpreter and loader control variable %s',
        (key) => {
            expect(isAllowedEnvKey(key)).toBe(false);
        }
    );

    it.each(['SPARK_APP_NAME', 'SPARK_HOME_DIR', 'MY_SPARK_HOME', 'PYSPARK_PYTHON2', 'SPARK', 'SPARK_JARS', 'CHECKPOINT_ROOT'])(
        'accepts %s because only exact Spark names are denied',
        (key) => {
            expect(isAllowedEnvKey(key)).toBe(true);
        }
    );

    it('skips every Spark denylist key in applyRuntimeVariables and keeps the base value', () => {
        const env: Record<string, string> = { SPARK_HOME: '/opt/spark' };
        const rejected = applyRuntimeVariables(
            env,
            [...SPARK_DENIED_KEYS.map((key) => ({ key, value: '/tmp/evil' })), { key: 'SPARK_APP_NAME', value: 'etl' }]
        );
        expect(rejected).toEqual(SPARK_DENIED_KEYS);
        expect(env).toEqual({ SPARK_HOME: '/opt/spark', SPARK_APP_NAME: 'etl' });
    });

    it('ignores null entries and non-string keys without throwing', () => {
        const env: Record<string, string> = {};
        const rejected = applyRuntimeVariables(env, [
            null as unknown as { key: string; value: string },
            { key: 42 as unknown as string, value: 'x' },
            { key: 'OK_KEY', value: 'v' },
        ]);
        expect(rejected).toEqual([]);
        expect(env).toEqual({ OK_KEY: 'v' });
    });

    it('lets a later duplicate key overwrite an earlier one', () => {
        const env: Record<string, string> = {};
        applyRuntimeVariables(env, [
            { key: 'DB_HOST', value: 'first' },
            { key: 'DB_HOST', value: 'second' },
        ]);
        expect(env).toEqual({ DB_HOST: 'second' });
    });
});
