/**
 * Integration Test Runner and Multi-Agent Evaluation Harness for Apache Spark Codegen.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Coordinates live end-to-end integration tests. The runner generates complete PySpark 4.x
 * repositories with Gemini and evaluates the output with a multi-agent evaluation harness
 * (Deterministic Gate, SparkArchitectureAgent, SecurityAndHardeningAgent,
 * PackagingAndDeployabilityAgent, PipelineSpecificationAgent, and Lead Arbiter).
 * It then writes the repository to a temporary directory and runs the generated
 * tests offline with the pytest runner. Failed, error, or timeout runs fail the test.
 * If local tooling is missing, the pytest step is skipped.
 *
 * Architectural decisions:
 * - Dynamically synchronizes pipelineConfig.language in the YAML specification to match
 *   the target generation language, eliminating language-mismatch cross-talk between tests.
 * - Replaces monolithic evaluation prompt with multi-agent specialist hierarchy.
 * - Builds the prompt with the SAME builder as the extension (`buildPromptFromPipelineYaml`).
 *   A private copy drifted in the past and re-opened fix F7 (version placeholder resolution).
 * - Emits actionable Prompt Remediation Directives that target a skill file under
 *   `resources/codegen-skills/` when tests fail, rather than patching generated test code.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFile } from 'child_process';
import * as yaml from 'yaml';
import { GoogleGenAI } from '@google/genai';
import { DEFAULT_SPARK_VERSION } from '../../shared/types';
import { DEFAULT_GEMINI_MODEL, PIPELINE_FILE_EXT } from '../../core/constants';
import { buildPromptFromPipelineYaml, PipelinePrompt } from '../../codegen/CodegenPromptBuilder';
import { CODEGEN_RESOURCE_PATHS, buildCodegenModelConfig } from '../../codegen/codegenConfig';
import { runMultiAgentEvaluation, formatEvaluationReport } from './evaluators/multi-agent-evaluator';
import { checkPythonSyntax } from './evaluators/deterministic-gate';
import { MultiAgentEvaluationResult, generateContentWithRetry } from './evaluators/types';
import { recordAndSaveTestReport } from './test-reporter';
import { runGeneratedRepoTests, formatPytestReport, PytestRunResult, PytestRunStatus } from './pytest-runner';

/** Wall-clock limit for the offline pytest run of one generated repository. */
export const OFFLINE_PYTEST_TIMEOUT_MS = 170_000;

export interface GeneratedFile {
    filename: string;
    content: string;
    executable?: boolean;
}

/**
 * Deduplicates generated files by filename, keeping the entry with the longest content
 * when a model emits a prematurely closed file object followed by a complete file entry.
 */
export function deduplicateGeneratedFiles(files: GeneratedFile[]): GeneratedFile[] {
    if (!Array.isArray(files)) {
        return [];
    }
    const byName = new Map<string, GeneratedFile>();
    for (const file of files) {
        if (!file || !file.filename || typeof file.content !== 'string') {
            continue;
        }
        const existing = byName.get(file.filename);
        if (!existing || file.content.length >= existing.content.length) {
            byName.set(file.filename, file);
        }
    }
    return Array.from(byName.values());
}

export interface EvaluationResult {
    isGood: boolean;
    issues: string[];
    explanation: string;
    multiAgentResult?: MultiAgentEvaluationResult;
}

export interface PipelineTestConfig {
    testName: string;
    /** Raw `.spark` pipeline file content. */
    sparkContent?: string;
    yamlContent?: string;
    languages?: ('Java' | 'Python')[];
}

/**
 * Recursively searches for a file with the given name within a directory.
 *
 * @param dir Directory path to search.
 * @param filename File name to locate.
 * @returns Absolute path to the file if found, otherwise null.
 */
function findFileRecursive(dir: string, filename: string): string | null {
    if (!fs.existsSync(dir)) return null;
    try {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
            const full = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                const found = findFileRecursive(full, filename);
                if (found) return found;
            } else if (entry.isFile() && entry.name === filename) {
                return full;
            }
        }
    } catch {
        // Ignore read errors
    }
    return null;
}

/**
 * Loads a `.spark` pipeline definition file from disk given a relative or absolute path.
 *
 * @param filePath Relative or absolute path, with or without the `.spark` extension.
 * @returns Raw pipeline YAML content.
 * @throws Error when no candidate path holds the file.
 */
export function loadSparkPipeline(filePath: string): string {
    const withExt = filePath.endsWith(PIPELINE_FILE_EXT) ? filePath : `${filePath}${PIPELINE_FILE_EXT}`;
    const candidatePaths = [
        filePath,
        path.isAbsolute(filePath) ? filePath : path.join(__dirname, filePath),
        path.join(__dirname, withExt),
        path.join(process.cwd(), 'src', 'test', 'integration', filePath),
        path.join(process.cwd(), 'src', 'test', 'integration', withExt)
    ];

    for (const p of candidatePaths) {
        if (fs.existsSync(p) && fs.statSync(p).isFile()) {
            return fs.readFileSync(p, 'utf-8');
        }
    }

    const found = findFileRecursive(path.join(process.cwd(), 'src', 'test', 'integration'), path.basename(withExt));
    if (found) {
        return fs.readFileSync(found, 'utf-8');
    }

    throw new Error(`Could not locate ${PIPELINE_FILE_EXT} pipeline file: ${filePath}`);
}

let cachedGcloudProject: string | undefined;

/**
 * Extracts the active GCP project ID from a gcloud configuration INI string.
 */
export function parseGcloudProjectFromIni(iniContent: string): string | undefined {
    const match = iniContent.match(/^\s*project\s*=\s*([^\s#\r\n]+)/m);
    return match?.[1]?.trim() || undefined;
}

/**
 * Attempts to read the active GCP project ID directly from local gcloud config files
 * without spawning a subprocess.
 */
export function readGcloudProjectFromDisk(): string | undefined {
    const envProject = process.env.GOOGLE_CLOUD_PROJECT || process.env.PROJECT_ID || process.env.GCLOUD_PROJECT;
    if (envProject && envProject.trim()) {
        return envProject.trim();
    }

    const configDir = process.env.CLOUDSDK_CONFIG || path.join(os.homedir(), '.config', 'gcloud');
    let activeConfigName = 'default';
    const activeConfigPath = path.join(configDir, 'active_config');
    try {
        if (fs.existsSync(activeConfigPath)) {
            const rawActive = fs.readFileSync(activeConfigPath, 'utf-8').trim();
            if (rawActive) {
                activeConfigName = rawActive;
            }
        }
        const configFilePath = path.join(configDir, 'configurations', `config_${activeConfigName}`);
        if (fs.existsSync(configFilePath)) {
            const parsed = parseGcloudProjectFromIni(fs.readFileSync(configFilePath, 'utf-8'));
            if (parsed) {
                return parsed;
            }
        }
    } catch {
        // Fallback to CLI
    }
    return undefined;
}

/**
 * Resolves the active GCP project ID from cache, local gcloud config, or gcloud CLI.
 *
 * @returns A promise resolving to the project ID or undefined if unavailable.
 */
export function getGcloudProject(): Promise<string | undefined> {
    if (cachedGcloudProject) {
        return Promise.resolve(cachedGcloudProject);
    }

    const diskProject = readGcloudProjectFromDisk();
    if (diskProject) {
        cachedGcloudProject = diskProject;
        return Promise.resolve(diskProject);
    }

    return new Promise((resolve) => {
        execFile('gcloud', ['config', 'get-value', 'project'], { timeout: 15000 }, (error, stdout) => {
            if (error || !stdout) {
                resolve(undefined);
            } else {
                const resolved = stdout.trim() || undefined;
                if (resolved) {
                    cachedGcloudProject = resolved;
                }
                resolve(resolved);
            }
        });
    });
}

/**
 * Locates the absolute path to the resources directory.
 *
 * @returns Absolute path to the resources directory.
 */
export function getResourcesRoot(): string {
    const candidatePaths = [
        path.join(__dirname, '..', '..', 'resources'),
        path.join(__dirname, '..', '..', '..', 'resources'),
        path.join(process.cwd(), 'resources')
    ];
    // NOTE: `src/test/integration/../../resources` is `src/resources` (the resource LOADER
    // code), which exists but holds no resources. Require a known resource file so the
    // lookup cannot stop at that folder.
    for (const p of candidatePaths) {
        if (fs.existsSync(path.join(p, CODEGEN_RESOURCE_PATHS.responseSchema))) {
            return p;
        }
    }
    throw new Error('Could not locate resources directory');
}

/**
 * Reads a resource file from the resources directory as a UTF-8 string.
 *
 * @param filePath Relative path to the resource file within the resources folder.
 * @returns Content of the resource file.
 */
export function readResource(filePath: string): string {
    const resourcesRoot = getResourcesRoot();
    const fullPath = path.join(resourcesRoot, filePath);
    return fs.readFileSync(fullPath, 'utf-8');
}

/**
 * Synchronizes the pipelineConfig.language property in the pipeline YAML
 * to match the target test language.
 *
 * @param yamlContent - Original YAML content.
 * @param targetLanguage - Target language ('Java' | 'Python').
 * @returns Synchronized YAML content string.
 */
export function synchronizePipelineLanguage(yamlContent: string, targetLanguage: 'Java' | 'Python'): string {
    if (!yamlContent) return yamlContent;
    try {
        const parsed = yaml.parse(yamlContent);
        if (parsed && typeof parsed === 'object') {
            if (!parsed.pipelineConfig) {
                parsed.pipelineConfig = {};
            }
            parsed.pipelineConfig.language = targetLanguage;
            return yaml.stringify(parsed);
        }
    } catch {
        return yamlContent.replace(/(language:\s*)[A-Za-z]+/g, `$1${targetLanguage}`);
    }
    return yamlContent;
}

/**
 * Builds the code generation prompt with the SAME shared builder as the extension
 * (`buildPromptFromPipelineYaml`: redaction, skill selection, assembly).
 *
 * @param language Target language.
 * @param yamlData Raw pipeline YAML.
 * @param sparkVersion Resolved Apache Spark version.
 * @returns The built prompt and the selected skill names.
 */
export function buildRunnerPrompt(_language: string, yamlData: string, sparkVersion?: string): PipelinePrompt {
    return buildPromptFromPipelineYaml({
        skillsRoot: path.join(getResourcesRoot(), CODEGEN_RESOURCE_PATHS.skillsDir),
        yamlText: yamlData,
        language: 'PySpark',
        sparkVersion,
    });
}

/**
 * Evaluates generated code using the Multi-Agent Evaluation Harness.
 *
 * @param ai - Initialized GoogleGenAI client.
 * @param codeGenPrompt - Full prompt template string.
 * @param files - Array of generated files.
 * @param language - Target language ('Java' | 'Python').
 * @param yamlContent - Original pipeline YAML.
 * @returns EvaluationResult with detailed scorecard and prompt directives.
 */
export async function evaluateGeneratedCode(
    ai: GoogleGenAI,
    codeGenPrompt: string,
    files: GeneratedFile[],
    language: 'Java' | 'Python' = 'Python',
    yamlContent = ''
): Promise<EvaluationResult> {
    const multiAgentResult = await runMultiAgentEvaluation(
        ai,
        language,
        yamlContent,
        codeGenPrompt,
        files
    );

    const report = formatEvaluationReport(multiAgentResult, language);
    console.log(report);

    return {
        isGood: multiAgentResult.isGood,
        issues: multiAgentResult.allFindings.map(
            f => `[${f.category}] ${f.severity}: ${f.description} (${f.file})`
        ),
        explanation: multiAgentResult.explanation,
        multiAgentResult
    };
}

/**
 * Initializes GoogleGenAI client with Vertex AI / active gcloud project.
 */
export async function createGeminiClient(): Promise<GoogleGenAI> {
    const projectId = (await getGcloudProject()) || process.env.GOOGLE_CLOUD_PROJECT || process.env.PROJECT_ID;
    const aiOptions: any = {};
    if (projectId) {
        aiOptions.vertexai = true;
        aiOptions.project = projectId;
        aiOptions.location = 'global';
    }
    return new GoogleGenAI(aiOptions);
}

/**
 * Checks whether generated Python files suffer from premature string closure or truncation.
 *
 * @param files Generated files.
 * @returns The first syntax error, or null when every Python file compiles.
 */
async function hasTruncatedSourceFile(files: GeneratedFile[]): Promise<string | null> {
    for (const f of files) {
        if (f.filename.endsWith('.py') && !f.filename.endsWith('__init__.py')) {
            const pyErr = await checkPythonSyntax(f.filename, f.content);
            if (pyErr) {
                return pyErr;
            }
        }
    }
    return null;
}

/**
 * Writes generated files below a repository root.
 *
 * @param files Generated files.
 * @param rootDir Existing repository root directory.
 * @returns Relative paths of the written files.
 * @throws Error if a filename is empty, absolute, or resolves outside `rootDir`.
 */
export function writeGeneratedRepo(files: GeneratedFile[], rootDir: string): string[] {
    const root = path.resolve(rootDir);
    const written: string[] = [];
    for (const file of files) {
        const name = (file.filename || '').trim();
        if (!name || path.isAbsolute(name)) {
            throw new Error(`Refusing to write generated file with an unsafe name: '${file.filename}'.`);
        }
        const target = path.resolve(root, name);
        const relative = path.relative(root, target);
        if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
            throw new Error(`Refusing to write generated file outside the repository: '${file.filename}'.`);
        }
        fs.mkdirSync(path.dirname(target), { recursive: true });
        fs.writeFileSync(target, file.content, 'utf8');
        if (file.executable) {
            fs.chmodSync(target, 0o755);
        }
        written.push(relative);
    }
    return written;
}

/**
 * Tells if an offline pytest status must fail the integration test.
 *
 * @param status Status from the pytest runner.
 * @returns True for `failed`, `error`, and `timeout`. False for `passed` and `skipped`.
 */
export function isBlockingPytestStatus(status: PytestRunStatus): boolean {
    return status === 'failed' || status === 'error' || status === 'timeout';
}

/**
 * Writes the generated repository to a temporary directory and runs its tests offline.
 *
 * If PySpark, pytest, or a compatible JDK is missing, the runner returns `skipped`.
 * The temporary directory is always removed.
 *
 * @param files Generated files.
 * @param python Python interpreter. Defaults to `SPARK_PYTEST_PYTHON` or `python3`.
 * @returns Typed pytest run result.
 */
export async function runOfflinePytest(
    files: GeneratedFile[],
    python: string = process.env.SPARK_PYTEST_PYTHON || 'python3'
): Promise<PytestRunResult> {
    const repoDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spark-gen-repo-'));
    try {
        writeGeneratedRepo(files, repoDir);
        return await runGeneratedRepoTests(repoDir, { python, timeoutMs: OFFLINE_PYTEST_TIMEOUT_MS });
    } finally {
        fs.rmSync(repoDir, { recursive: true, force: true });
    }
}

/**
 * Executes an end-to-end code generation and validation test for a pipeline YAML across specified languages.
 */
export async function runPipelineIntegrationTest(config: PipelineTestConfig): Promise<{ [key: string]: boolean }> {
    const languages = config.languages || ['Python'];
    const ai = await createGeminiClient();
    const sparkVersion = DEFAULT_SPARK_VERSION;
    const results: { [key: string]: boolean } = {};

    console.log(`\n======================================================`);
    console.log(`🧪 Running Pipeline Integration Test: ${config.testName}`);
    console.log(`🤖 Using Gemini Model: ${DEFAULT_GEMINI_MODEL} (Apache Spark: ${sparkVersion})`);
    console.log(`======================================================`);

    await Promise.all(languages.map(async (language) => {
        console.log(`\n------------------------------------------------------`);
        console.log(`▶ Testing ${language} Pipeline Code Generation`);
        console.log(`------------------------------------------------------`);

        try {
            const rawContent = config.sparkContent || config.yamlContent;
            if (!rawContent) {
                throw new Error(`Pipeline content is required (provide sparkContent or yamlContent) for ${config.testName}`);
            }

            // Synchronize pipeline language with current target language
            const pipelineContent = synchronizePipelineLanguage(rawContent, language);
            // Same prompt builder as the extension (redaction + skill selection + assembly).
            const built = buildRunnerPrompt(language, pipelineContent, sparkVersion);
            const prompt = built.prompt;
            console.log(`   Selected skills (${built.selectedSkills.length}): ${built.selectedSkills.join(', ')}`);
            const responseSchema = JSON.parse(readResource(CODEGEN_RESOURCE_PATHS.responseSchema));

            console.log(`1. Generating ${language} code via Gemini (${DEFAULT_GEMINI_MODEL})...`);
            const startTime = Date.now();
            let files: GeneratedFile[] = [];

            for (let genAttempt = 1; genAttempt <= 2; genAttempt++) {
                const response = await generateContentWithRetry(ai, {
                    model: DEFAULT_GEMINI_MODEL,
                    contents: prompt,
                    config: buildCodegenModelConfig(responseSchema)
                });
                console.log(`   Generated in ${((Date.now() - startTime) / 1000).toFixed(1)}s`);

                const finishReason = response.candidates?.[0]?.finishReason;
                if (finishReason === 'MAX_TOKENS') {
                    console.warn(`   [CodeGen] ⚠️ Gemini response hit MAX_TOKENS limit on attempt ${genAttempt}.`);
                }

                if (!response.text) {
                    throw new Error(`Empty response received from Gemini during ${language} generation`);
                }

                const parsed = JSON.parse(response.text);
                files = deduplicateGeneratedFiles(parsed.files || []);

                const truncationErr = await hasTruncatedSourceFile(files);
                if ((finishReason === 'MAX_TOKENS' || truncationErr) && genAttempt < 2) {
                    console.warn(`   [CodeGen] ⚠️ Structural/truncation issue detected (${truncationErr || finishReason}). Retrying generation once...`);
                    continue;
                }
                break;
            }

            console.log(`   Generated ${files.length} files:`, files.map(f => f.filename).join(', '));

            const mainFile = files.find(f => f.filename.endsWith('.py') && !f.filename.includes('test') && !f.filename.endsWith('__init__.py'));

            if (mainFile) {
                console.log(`\n📄 Generated Main File (${mainFile.filename}):\n`);
            }

            console.log(`\n2. Evaluating generated ${language} code via Multi-Agent Evaluation Harness...`);
            const evalResult = await evaluateGeneratedCode(ai, prompt, files, language, pipelineContent);

            console.log(`\n📋 Review Results for ${language}:`);
            console.log(`   Status: ${evalResult.isGood ? '✅ PASS' : '❌ FAIL'}`);
            console.log(`   Explanation: ${evalResult.explanation}`);
            if (evalResult.issues.length > 0) {
                console.log(`   Issues Reported:`);
                evalResult.issues.forEach((issue, idx) => console.log(`     ${idx + 1}. ${issue}`));
            }

            console.log(`\n3. Running generated tests offline with local PySpark...`);
            const pytestResult = await runOfflinePytest(files);
            const pytestReport = formatPytestReport(pytestResult);
            console.log(pytestReport);
            const pytestBlocks = isBlockingPytestStatus(pytestResult.status);
            const passed = evalResult.isGood && !pytestBlocks;
            const explanation = pytestBlocks
                ? `${evalResult.explanation}\n\n${pytestReport}`
                : evalResult.explanation;

            // Persist evaluation findings, scorecards, and prompt directives to test-report.md
            const reportPath = recordAndSaveTestReport({
                testName: config.testName,
                language: language,
                timestamp: new Date().toISOString(),
                passed,
                durationMs: Date.now() - startTime,
                scorecard: evalResult.multiAgentResult?.scorecard,
                hardVetoTriggered: evalResult.multiAgentResult?.hardVetoTriggered,
                hardVetoReasons: evalResult.multiAgentResult?.hardVetoReasons,
                explanation,
                findings: evalResult.multiAgentResult?.confirmedFindings || evalResult.multiAgentResult?.allFindings || [],
                promptRemediationDirectives: evalResult.multiAgentResult?.promptRemediationDirectives || [],
                generatedFiles: files.map(f => ({
                    filename: f.filename,
                    lines: f.content.split('\n').length
                }))
            });
            console.log(`\n📄 Test report updated: ${reportPath}`);

            results[language] = passed;
        } catch (err: any) {
            console.error(`❌ Error during ${language} test:`, err.message || err);
            const reportPath = recordAndSaveTestReport({
                testName: config.testName,
                language: language,
                timestamp: new Date().toISOString(),
                passed: false,
                explanation: `Execution error: ${err.message || err}`,
                findings: [],
                promptRemediationDirectives: [],
                generatedFiles: [],
                error: err.message || String(err)
            });
            console.log(`\n📄 Test report updated: ${reportPath}`);
            results[language] = false;
        }
    }));

    console.log(`\n======================================================`);
    console.log(`📊 SUMMARY: ${config.testName}`);
    console.log(`======================================================`);
    for (const [lang, passed] of Object.entries(results)) {
        console.log(`${lang.padEnd(8)}: ${passed ? '✅ PASSED' : '❌ FAILED'}`);
    }

    const allPassed = Object.values(results).every(v => v === true);
    if (!allPassed) {
        throw new Error(`Integration test failed for ${config.testName}`);
    }

    return results;
}
