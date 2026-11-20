/**
 * Skill File Parser
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Parses one bundled code generation skill file
 * (`resources/codegen-skills/<name>/SKILL.md`): a YAML-style frontmatter block between
 * `---` lines, followed by the markdown body.
 *
 * Supported frontmatter (the only shape that the skills use; anything else fails fast):
 * ```yaml
 * ---
 * name: core-pyspark
 * description: Core PySpark 4.x session initialization, Secret Manager, and DLQ split rules.
 * metadata:
 *   kind: language
 *   usedBy: [generator, reviewer]
 *   languages: [python]
 *   runners: [all]
 *   paradigms: [imperative, sdp]
 *   transforms: []
 *   preloadWhen: []
 *   sources:
 *     - prompt
 *     - https://spark.apache.org/docs/latest/api/python/
 *   validated: true
 * ---
 * ```
 *
 * Architectural decisions / notes for future sessions:
 * - A small dedicated parser instead of a YAML dependency. The format is fully under our
 *   control, and strict parsing surfaces malformed skills at load time.
 * - Unknown top-level keys and unknown metadata keys throw. A typo such as `preloadwhen`
 *   would otherwise silently disable a skill.
 * - Lists use flow style (`[a, b]`) or block style (`- a` lines). Block style keeps long
 *   `sources` URL lists readable.
 * - The body may contain `---` Markdown rules. Only the FIRST closing `---` ends the
 *   frontmatter.
 */

/** A parsed skill file. Values are raw strings or string lists; the catalog validates them. */
export interface ParsedSkillFile {
    /** Skill id. Equals the folder name. */
    name: string;
    /** One-sentence description. */
    description: string;
    /** Raw `metadata` map. */
    metadata: Record<string, string | string[]>;
    /** Markdown body after the frontmatter, trimmed. */
    body: string;
}

/** Line that opens and closes the frontmatter block. */
const FRONTMATTER_DELIMITER = '---';

/** Top-level frontmatter keys. */
export const SKILL_TOP_LEVEL_KEYS: ReadonlySet<string> = new Set(['name', 'description', 'metadata']);

/** Keys allowed inside `metadata`. */
export const SKILL_METADATA_KEYS: ReadonlySet<string> = new Set([
    'kind',
    'usedBy',
    'languages',
    'runners',
    'paradigms',
    'transforms',
    'preloadWhen',
    'sources',
    'validated',
]);

/** `key: value` at column 0. */
const TOP_LEVEL_LINE = /^([A-Za-z][\w-]*):\s*(.*)$/;
/** `  key: value` indented by at least two spaces (inside `metadata`). */
const METADATA_LINE = /^ {2,}([A-Za-z][\w-]*):\s*(.*)$/;
/** `    - value` block list item (inside a metadata key). */
const BLOCK_LIST_ITEM = /^ {2,}-\s+(.*)$/;
/** `[a, b]` flow list. */
const FLOW_LIST = /^\[(.*)\]$/;

/**
 * Removes one pair of matching surrounding quotes from a value.
 *
 * @param value Trimmed value text.
 * @returns The value without its surrounding quotes.
 */
function unquote(value: string): string {
    const quoted = (value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"));
    return quoted && value.length >= 2 ? value.slice(1, -1).replace(/\\"/g, '"') : value;
}

/**
 * Parses a scalar or flow-list value (`"quoted"`, `plain`, `[a, b]`).
 *
 * @param raw Raw value text.
 * @param sourceLabel Label used in error messages.
 * @returns A string, or a string list for flow lists.
 * @throws Error when a flow list is not closed.
 */
function parseValue(raw: string, sourceLabel: string): string | string[] {
    const value = raw.trim();
    // Flow list: split on commas and unquote each entry. Empty entries are dropped.
    const list = FLOW_LIST.exec(value);
    if (list) {
        return list[1]
            .split(',')
            .map((item) => unquote(item.trim()))
            .filter((item) => item.length > 0);
    }
    // A value that opens a list but does not close it is a malformed list.
    if (value.startsWith('[')) {
        throw new Error(`${sourceLabel}: malformed list value '${value}'. Use [a, b].`);
    }
    return unquote(value);
}

/**
 * Parses a skill file.
 *
 * @param content File content (LF or CRLF line endings).
 * @param sourceLabel Label used in error messages (for example the file path).
 * @param expectedName Optional folder name. If given, `name` must equal it.
 * @returns The parsed skill file.
 * @throws Error when the frontmatter is missing, malformed, has unknown keys, or the
 *   name does not match the folder.
 */
export function parseSkillFile(content: string, sourceLabel: string, expectedName?: string): ParsedSkillFile {
    // 1. Normalize line endings and locate the frontmatter block.
    const lines = content.replace(/\r\n/g, '\n').split('\n');
    if (lines[0]?.trim() !== FRONTMATTER_DELIMITER) {
        throw new Error(`${sourceLabel}: skill file must start with a '---' frontmatter block.`);
    }
    const end = lines.findIndex((line, index) => index > 0 && line.trim() === FRONTMATTER_DELIMITER);
    if (end < 0) {
        throw new Error(`${sourceLabel}: frontmatter block is not closed with '---'.`);
    }

    // 2. Walk the frontmatter lines. `metadata:` opens a nested map; a metadata key with
    //    an empty value opens a block list.
    const top: Record<string, string | string[]> = {};
    const metadata: Record<string, string | string[]> = {};
    let inMetadata = false;
    let openListKey: string | undefined;
    for (const line of lines.slice(1, end)) {
        if (!line.trim()) {
            continue;
        }
        // 2a. Block list item under the open metadata key.
        const item = BLOCK_LIST_ITEM.exec(line);
        if (inMetadata && item) {
            if (!openListKey) {
                throw new Error(`${sourceLabel}: list item '${line.trim()}' has no parent key.`);
            }
            (metadata[openListKey] as string[]).push(unquote(item[1].trim()));
            continue;
        }
        // 2b. Nested metadata key.
        const nested = METADATA_LINE.exec(line);
        if (inMetadata && nested) {
            const key = nested[1];
            if (!SKILL_METADATA_KEYS.has(key)) {
                throw new Error(`${sourceLabel}: unknown metadata key '${key}'. Allowed: ${[...SKILL_METADATA_KEYS].join(', ')}.`);
            }
            if (nested[2].trim() === '') {
                metadata[key] = [];
                openListKey = key;
            } else {
                metadata[key] = parseValue(nested[2], sourceLabel);
                openListKey = undefined;
            }
            continue;
        }
        // 2c. Top-level key.
        const topMatch = TOP_LEVEL_LINE.exec(line);
        if (!topMatch) {
            throw new Error(`${sourceLabel}: unsupported frontmatter line '${line.trim()}'.`);
        }
        const key = topMatch[1];
        if (!SKILL_TOP_LEVEL_KEYS.has(key)) {
            throw new Error(`${sourceLabel}: unknown frontmatter key '${key}'. Allowed: ${[...SKILL_TOP_LEVEL_KEYS].join(', ')}.`);
        }
        openListKey = undefined;
        inMetadata = key === 'metadata';
        if (inMetadata && topMatch[2].trim() !== '') {
            throw new Error(`${sourceLabel}: 'metadata' must be a nested map.`);
        }
        if (!inMetadata) {
            top[key] = parseValue(topMatch[2], sourceLabel);
        }
    }

    // 3. Validate the required scalar keys and the folder name.
    const { name, description } = top;
    if (typeof name !== 'string' || !name) {
        throw new Error(`${sourceLabel}: frontmatter 'name' is required.`);
    }
    if (typeof description !== 'string' || !description) {
        throw new Error(`${sourceLabel}: frontmatter 'description' is required.`);
    }
    if (expectedName !== undefined && name !== expectedName) {
        throw new Error(`${sourceLabel}: skill name '${name}' must equal its folder name '${expectedName}'.`);
    }
    return { name, description, metadata, body: lines.slice(end + 1).join('\n').trim() };
}
