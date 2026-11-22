/**
 * Unit tests for `resolveResourcePath` (`src/resources/resourceLoader.ts`), which locates the
 * `resources/codegen-skills` folder for the skill catalog.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import * as path from 'path';

vi.mock('vscode', async () => (await import('../helpers/vscodeMock')).createVscodeMock());

import { resolveResourcePath, readResourceFile } from '../../../resources/resourceLoader';

/** Repository root. */
const REPO_ROOT = path.join(__dirname, '..', '..', '..', '..');

describe('resolveResourcePath', () => {
    it('returns the resources/ path of an existing folder (simple)', () => {
        const context = { asAbsolutePath: (rel: string) => path.join(REPO_ROOT, rel) } as any;
        expect(resolveResourcePath(context, 'codegen-skills')).toBe(path.join(REPO_ROOT, 'resources', 'codegen-skills'));
    });

    it('throws with every checked path when nothing exists (failure)', () => {
        const context = { asAbsolutePath: (rel: string) => path.join('/nonexistent-root', rel) } as any;
        expect(() => resolveResourcePath(context, 'no-such-folder')).toThrow(/Checked paths: .*no-such-folder/);
        expect(() => readResourceFile(context, 'no-such-file.json')).toThrow(/Resource file not found/);
    });
});
