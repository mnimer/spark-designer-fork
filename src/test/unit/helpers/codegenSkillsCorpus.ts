/**
 * Codegen Skills Corpus (test helper)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Prompt-content unit tests used to read one monolithic prompt file per language. The prompt
 * text now lives in many skills (`resources/codegen-skills/**`). This helper returns the
 * concatenated text of all skills, of the skills for one language, or of one skill, so the
 * content assertions keep working no matter which skill holds a rule.
 *
 * Architectural decisions / notes for future sessions:
 * - The corpus reads files directly (no catalog), so a malformed frontmatter cannot hide a
 *   content regression. The invariant suite tests the frontmatter separately.
 * - A language corpus includes every `SKILL.md`, the reference files of that language, and
 *   the runner templates of that language. It excludes skills limited to another language.
 */

import * as fs from 'fs';
import * as path from 'path';

/** Absolute path of the bundled skills folder. */
export const SKILLS_ROOT = path.join(__dirname, '..', '..', '..', '..', 'resources', 'codegen-skills');

/** Skill language ids used in file names (`python.md`, `templates/python/`). */
export type CorpusLanguage = 'python' | 'java';

/**
 * Lists every skill folder name, sorted.
 *
 * @returns Skill folder names.
 */
export function listSkillFolders(): string[] {
    return fs
        .readdirSync(SKILLS_ROOT, { withFileTypes: true })
        .filter((entry) => entry.isDirectory())
        .map((entry) => entry.name)
        .sort();
}

/**
 * Reads the `languages:` frontmatter list of a skill without the catalog.
 *
 * @param skillMd SKILL.md text.
 * @returns The listed languages (defaults to `all`).
 */
function readLanguages(skillMd: string): string[] {
    const match = /^\s+languages:\s*\[([^\]]*)\]/m.exec(skillMd);
    return match ? match[1].split(',').map((value) => value.trim()) : ['all'];
}

/**
 * Lists every file of a skill folder, recursively, sorted.
 *
 * @param dir Absolute folder path.
 * @returns Absolute file paths.
 */
function walk(dir: string): string[] {
    return fs
        .readdirSync(dir, { withFileTypes: true })
        .flatMap((entry) => (entry.isDirectory() ? walk(path.join(dir, entry.name)) : [path.join(dir, entry.name)]))
        .sort();
}

/**
 * Returns the text of one skill: SKILL.md, plus the language reference file and the
 * language templates when a language is given (all files when it is not).
 *
 * @param name Skill folder name.
 * @param language Optional language filter.
 * @returns Concatenated text.
 */
export function skillText(name: string, language?: CorpusLanguage): string {
    const dir = path.join(SKILLS_ROOT, name);
    return walk(dir)
        .filter((file) => {
            if (!language) {
                return true;
            }
            const rel = path.relative(dir, file).split(path.sep);
            const otherLanguage = (id: string) => id !== language && (id === 'python' || id === 'java');
            // Drop the other language's reference file and templates.
            if (rel.length === 1 && rel[0].endsWith('.md') && otherLanguage(rel[0].replace(/\.md$/, ''))) {
                return false;
            }
            return !(rel[0] === 'templates' && otherLanguage(rel[1]));
        })
        .map((file) => fs.readFileSync(file, 'utf-8'))
        .join('\n\n');
}

/**
 * Returns the concatenated text of every skill, or of the skills for one language.
 *
 * @param language Optional language filter.
 * @returns Concatenated corpus text.
 */
export function skillsCorpus(language?: CorpusLanguage): string {
    return listSkillFolders()
        .filter((name) => {
            if (!language) {
                return true;
            }
            const languages = readLanguages(fs.readFileSync(path.join(SKILLS_ROOT, name, 'SKILL.md'), 'utf-8'));
            return languages.includes('all') || languages.includes(language);
        })
        .map((name) => skillText(name, language))
        .join('\n\n');
}
