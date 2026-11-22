/**
 * @fileoverview Unit tests for SourceCodeTab and GeneratorSettings components.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SourceCodeTab, highlightCode } from './SourceCodeTab';
import {
  GeneratorSettings,
  CODEGEN_REVIEWER_ENABLED_BY_DEFAULT,
  PYSPARK_RUNTIME_BADGE_LABEL,
} from './sourceCode/GeneratorSettings';
import { renderWithBottomPanel } from './testUtils';

describe('SourceCodeTab & GeneratorSettings', () => {
  const mockProps = {
    generatedCode: 'print("pipeline")',
    language: 'Python' as const,
    setLanguage: vi.fn(),
    outputFolder: 'src/output',
    setOutputFolder: vi.fn(),
    onPickFolder: vi.fn(),
    onRegenerate: vi.fn(),
    needsRebuild: false,
    files: ['job.py', 'requirements.txt'],
    selectedFile: null,
    onSelectFile: vi.fn(),
    selectedFileContent: null,
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders read-only PySpark (Spark 4.x) badge and hides legacy Python/Java radio buttons (G12)', () => {
    renderWithBottomPanel(<SourceCodeTab />, mockProps);

    // Check read-only PySpark runtime badge
    const runtimeBadge = screen.getByTestId('pyspark-runtime-badge');
    expect(runtimeBadge).toBeInTheDocument();
    expect(runtimeBadge).toHaveTextContent(PYSPARK_RUNTIME_BADGE_LABEL);
    expect(runtimeBadge).toHaveTextContent('Python 3.12+');

    // Legacy radio buttons for Python / Java should not exist
    expect(screen.queryByRole('radio', { name: /Python/i })).not.toBeInTheDocument();
    expect(screen.queryByRole('radio', { name: /Java/i })).not.toBeInTheDocument();

    // Check folder inputs
    expect(screen.getByPlaceholderText('./scratch_flow/')).toHaveValue('src/output');
    expect(screen.getByText('Valid PySpark package name: src/output')).toBeInTheDocument();

    // Check action buttons
    expect(screen.getByText('Generate Code')).toBeInTheDocument();
    expect(screen.getByText('Stop')).toBeInTheDocument();

    // Check vertical generated files list shows all generated files with badges
    const sidebar = screen.getByTestId('generated-files-sidebar-list');
    expect(sidebar).toBeInTheDocument();
    expect(sidebar).toHaveTextContent('Generated Files (2)');
    expect(screen.getByText('job.py')).toBeInTheDocument();
    expect(screen.getByText('requirements.txt')).toBeInTheDocument();

    // Check top header bar shows active filename, model badge, and action buttons
    expect(screen.getByTestId('active-file-header-name')).toHaveTextContent('job.py');
    expect(screen.getByTestId('active-model-badge')).toHaveTextContent('Model: gemini-4-hydrogen');
    expect(screen.getByTestId('find-in-code-btn')).toBeInTheDocument();
    expect(screen.getByTestId('copy-code-btn')).toBeInTheDocument();
    expect(screen.getByTestId('download-code-btn')).toBeInTheDocument();

    // Default code should be visible
    expect(
      screen.getByText((_content, node) => {
        return node?.tagName === 'CODE' && node?.textContent === 'print("pipeline")';
      }),
    ).toBeInTheDocument();
  });

  it('defaults AI Code Reviewer Pass toggle to off and passes enableReviewer state in onRegenerate (G12)', () => {
    const onRegenerate = vi.fn();
    const setEnableReviewer = vi.fn();
    renderWithBottomPanel(<SourceCodeTab />, {
      ...mockProps,
      onRegenerate,
      setEnableReviewer,
    });

    expect(CODEGEN_REVIEWER_ENABLED_BY_DEFAULT).toBe(false);

    const reviewerToggle = screen.getByTestId('ai-reviewer-toggle') as HTMLInputElement;
    expect(reviewerToggle).toBeInTheDocument();
    expect(reviewerToggle.checked).toBe(false);

    // Clicking Generate Code with default (false) passes { enableReviewer: false, model: 'gemini-4-hydrogen' }
    fireEvent.click(screen.getByText('Generate Code'));
    expect(onRegenerate).toHaveBeenCalledWith({
      enableReviewer: false,
      model: 'gemini-4-hydrogen',
    });

    // Toggling the AI Reviewer switch on updates state and passes { enableReviewer: true, model: 'gemini-4-hydrogen' }
    fireEvent.click(reviewerToggle);
    expect(setEnableReviewer).toHaveBeenCalledWith(true);
    expect(reviewerToggle.checked).toBe(true);

    fireEvent.click(screen.getByText('Generate Code'));
    expect(onRegenerate).toHaveBeenLastCalledWith({
      enableReviewer: true,
      model: 'gemini-4-hydrogen',
    });
  });

  it('respects controlled enableReviewer value from BottomPanelContext and disables toggle while generating (G12)', () => {
    const onRegenerate = vi.fn();
    const { rerenderWithContext } = renderWithBottomPanel(<SourceCodeTab />, {
      ...mockProps,
      onRegenerate,
      enableReviewer: true,
    });

    const reviewerToggle = screen.getByTestId('ai-reviewer-toggle') as HTMLInputElement;
    expect(reviewerToggle.checked).toBe(true);

    fireEvent.click(screen.getByText('Generate Code'));
    expect(onRegenerate).toHaveBeenCalledWith({
      enableReviewer: true,
      model: 'gemini-4-hydrogen',
    });

    // Re-render with isGenerating: true -> toggle should be disabled
    rerenderWithContext(<SourceCodeTab />, {
      ...mockProps,
      onRegenerate,
      enableReviewer: true,
      isGenerating: true,
      generationProgress: 'Reviewer step 1/2: running py_compile and pytest checks...',
    });

    expect(screen.getByTestId('ai-reviewer-toggle')).toBeDisabled();
    expect(screen.getByTestId('generation-progress')).toHaveTextContent(
      'Reviewer step 1/2: running py_compile and pytest checks...',
    );
  });

  it('renders GeneratorSettings directly with unsupportedNodes SDK opt-in and Reviewer toggle', () => {
    const onRegenerate = vi.fn();
    const onEnableReviewerChange = vi.fn();
    const onUseSdkChange = vi.fn();

    renderWithBottomPanel(
      <GeneratorSettings
        outputFolder="./my_spark_job/"
        setOutputFolder={vi.fn()}
        onPickFolder={vi.fn()}
        onRegenerate={onRegenerate}
        effectiveIsGraphValid={true}
        effectiveValidationErrors={[]}
        unsupportedNodes={[
          { id: 'n1', label: 'Pub/Sub Source', sparkTransform: 'PubSubSource' },
        ]}
        useSdkForUnsupportedNodes={true}
        onUseSdkForUnsupportedNodesChange={onUseSdkChange}
        enableReviewer={false}
        onEnableReviewerChange={onEnableReviewerChange}
        packageDisplayName="my_spark_job"
      />,
      mockProps,
    );

    expect(screen.getByTestId('pyspark-runtime-badge')).toHaveTextContent('PySpark (Spark 4.x)');
    expect(screen.getByTestId('use-sdk-for-unsupported')).toBeInTheDocument();
    expect(screen.getByTestId('use-sdk-notice')).toHaveTextContent('Pub/Sub Source');

    fireEvent.click(screen.getByTestId('ai-reviewer-toggle'));
    expect(onEnableReviewerChange).toHaveBeenCalledWith(true);
  });

  it('displays color-coded file type badges in the vertical file list and sub-header', () => {
    renderWithBottomPanel(<SourceCodeTab />, {
      ...mockProps,
      files: ['pipeline.py', 'test_pipeline.py', 'requirements.txt', 'README.md', 'Dockerfile'],
      selectedFile: 'requirements.txt',
      selectedFileContent: 'pyspark==4.0.0',
    });

    expect(screen.getByTestId('generated-file-item-pipeline.py')).toHaveTextContent('PY');
    expect(screen.getByTestId('generated-file-item-test_pipeline.py')).toHaveTextContent('TEST');
    expect(screen.getByTestId('generated-file-item-requirements.txt')).toHaveTextContent('DEPS');
    expect(screen.getByTestId('generated-file-item-README.md')).toHaveTextContent('DOC');
    expect(screen.getByTestId('generated-file-item-Dockerfile')).toHaveTextContent('DOCKER');
    expect(screen.getByTestId('active-file-type-badge')).toHaveTextContent('DEPS');
    expect(screen.getByText('Python runtime dependencies')).toBeInTheDocument();
  });

  it('toggles inline Find bar, highlights matches in active file, and closes on Escape', () => {
    renderWithBottomPanel(<SourceCodeTab />, {
      ...mockProps,
      generatedCode: 'from pyspark.sql import SparkSession\nSparkSession.builder.getOrCreate()',
    });

    expect(screen.queryByTestId('find-in-files-bar')).not.toBeInTheDocument();

    // Open Find bar via button
    fireEvent.click(screen.getByTestId('find-in-code-btn'));
    expect(screen.getByTestId('find-in-files-bar')).toBeInTheDocument();

    const findInput = screen.getByTestId('find-in-files-input');
    fireEvent.change(findInput, { target: { value: 'SparkSession' } });

    expect(screen.getByTestId('find-match-count')).toHaveTextContent('2 matches');
    expect(screen.getAllByTestId('search-match-highlight')).toHaveLength(2);

    // Close via Escape key
    fireEvent.keyDown(findInput, { key: 'Escape' });
    expect(screen.queryByTestId('find-in-files-bar')).not.toBeInTheDocument();
  });

  it('copies active file content to clipboard when clicking Copy button', () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: { writeText: writeTextMock },
    });

    renderWithBottomPanel(<SourceCodeTab />, mockProps);

    const copyBtn = screen.getByTestId('copy-code-btn');
    fireEvent.click(copyBtn);

    expect(writeTextMock).toHaveBeenCalledWith('print("pipeline")');
    expect(copyBtn).toHaveTextContent('Copied');
  });

  it('filters out venv, node_modules, and build cache files from the file list', () => {
    renderWithBottomPanel(<SourceCodeTab />, {
      ...mockProps,
      files: [
        'pipeline.py',
        'venv/lib/python3.11/site-packages/pyspark.py',
        'node_modules/pkg/index.js',
        '.venv/lib/site-packages/test.py',
        '__pycache__/pipeline.pyc',
      ],
    });

    expect(screen.getByText('pipeline.py')).toBeInTheDocument();
    expect(screen.queryByText('venv/lib/python3.11/site-packages/pyspark.py')).not.toBeInTheDocument();
    expect(screen.queryByText('.venv/lib/site-packages/test.py')).not.toBeInTheDocument();
  });

  it('calls onStopGeneration when clicking Stop button', () => {
    const onStopGeneration = vi.fn();
    renderWithBottomPanel(<SourceCodeTab />, { ...mockProps, isGenerating: true, onStopGeneration });

    const stopBtn = screen.getByText('Stop');
    fireEvent.click(stopBtn);
    expect(onStopGeneration).toHaveBeenCalled();
  });

  it('calls setOutputFolder when typing in folder input', () => {
    renderWithBottomPanel(<SourceCodeTab />, mockProps);

    const input = screen.getByPlaceholderText('./scratch_flow/');
    fireEvent.change(input, { target: { value: 'new/path' } });
    expect(mockProps.setOutputFolder).toHaveBeenCalledWith('new/path');
  });

  it('calls onPickFolder when clicking browse folder button', () => {
    renderWithBottomPanel(<SourceCodeTab />, mockProps);

    const button = screen.getByTitle('Browse Folder');
    fireEvent.click(button);
    expect(mockProps.onPickFolder).toHaveBeenCalled();
  });

  it('calls onRegenerate when clicking regenerate button', () => {
    renderWithBottomPanel(<SourceCodeTab />, mockProps);

    fireEvent.click(screen.getByText('Generate Code'));
    expect(mockProps.onRegenerate).toHaveBeenCalledWith({
      enableReviewer: false,
      model: 'gemini-4-hydrogen',
    });
  });

  describe('Phase 14 Dynamic Codegen Model Selector & Active Badge (MOD1)', () => {
    const vsWindow = window as unknown as {
      vscode?: { postMessage: ReturnType<typeof vi.fn> };
    };

    it('populates codegen-model-select from codegenModelsLoaded, auto-selects Flash default, updates active-model-badge, and forwards model to onRegenerate', () => {
      const mockPostMessage = vi.fn();
      vsWindow.vscode = { postMessage: mockPostMessage };
      const onRegenerate = vi.fn();
      const setSelectedModel = vi.fn();

      try {
        renderWithBottomPanel(<SourceCodeTab />, {
          ...mockProps,
          projectId: 'my-gcp-project',
          onRegenerate,
          setSelectedModel,
        });

        // Initial mount sends getCodegenModels with valid projectId and displays fallback gemini-4-hydrogen
        expect(mockPostMessage).toHaveBeenCalledTimes(1);
        expect(mockPostMessage).toHaveBeenCalledWith({
          command: 'getCodegenModels',
          projectId: 'my-gcp-project',
        });

        const modelSelect = screen.getByLabelText(/CODEGEN MODEL/i) as HTMLSelectElement;
        expect(modelSelect).toBe(screen.getByTestId('codegen-model-select'));
        expect(modelSelect.value).toBe('gemini-4-hydrogen');
        expect(screen.getByTestId('active-model-badge')).toHaveTextContent(
          'Model: gemini-4-hydrogen',
        );

        // Host responds with ranked catalog where a newer Flash release gemini-4.1-flash leads gemini-4-hydrogen and gemini-3.8-flash
        act(() => {
          window.dispatchEvent(
            new MessageEvent('message', {
              data: {
                command: 'codegenModelsLoaded',
                models: [
                  {
                    id: 'gemini-4.1-flash',
                    displayName: 'Gemini 4.1 Flash',
                    tier: 'flash',
                    version: '4.1',
                    isDefault: true,
                  },
                  {
                    id: 'gemini-4-hydrogen',
                    displayName: 'Gemini 4 Hydrogen',
                    tier: 'flash',
                    version: '4.0',
                  },
                  {
                    id: 'gemini-3.8-flash',
                    displayName: 'Gemini 3.8 Flash',
                    tier: 'flash',
                    version: '3.8',
                  },
                  {
                    id: 'gemini-4-carbon',
                    displayName: 'Gemini 4 Carbon',
                    tier: 'pro',
                    version: '4.0',
                  },
                ],
                requestParams: { projectId: 'my-gcp-project' },
              },
            }),
          );
        });

        expect(modelSelect.value).toBe('gemini-4.1-flash');
        expect(setSelectedModel).toHaveBeenCalledWith('gemini-4.1-flash');
        expect(screen.getByTestId('active-model-badge')).toHaveTextContent(
          'Model: gemini-4.1-flash',
        );
        expect(
          screen.getByRole('option', { name: 'Gemini 4.1 Flash (gemini-4.1-flash)' }),
        ).toBeInTheDocument();
        expect(
          screen.getByRole('option', { name: 'Gemini 4 Hydrogen (gemini-4-hydrogen)' }),
        ).toBeInTheDocument();
        expect(
          screen.getByRole('option', { name: 'Gemini 4 Carbon (gemini-4-carbon)' }),
        ).toBeInTheDocument();

        fireEvent.click(screen.getByText('Generate Code'));
        expect(onRegenerate).toHaveBeenCalledWith({
          enableReviewer: false,
          model: 'gemini-4.1-flash',
        });
      } finally {
        delete vsWindow.vscode;
      }
    });

    it('preserves manual user model choice across Refresh and falls back to Flash default only when removed from catalog', () => {
      const mockPostMessage = vi.fn();
      vsWindow.vscode = { postMessage: mockPostMessage };
      const onRegenerate = vi.fn();

      try {
        renderWithBottomPanel(<SourceCodeTab />, {
          ...mockProps,
          projectId: 'YOUR_PROJECT_ID',
          onRegenerate,
        });

        // Placeholder YOUR_PROJECT_ID is omitted from getCodegenModels
        expect(mockPostMessage).toHaveBeenCalledWith({ command: 'getCodegenModels' });

        const fullCatalog = [
          {
            id: 'gemini-4-hydrogen',
            displayName: 'Gemini 4 Hydrogen',
            tier: 'flash' as const,
            version: '4.0',
            isDefault: true,
          },
          {
            id: 'gemini-3.8-flash',
            displayName: 'Gemini 3.8 Flash',
            tier: 'flash' as const,
            version: '3.8',
          },
          {
            id: 'gemini-4-carbon',
            displayName: 'Gemini 4 Carbon',
            tier: 'pro' as const,
            version: '4.0',
          },
        ];

        act(() => {
          window.dispatchEvent(
            new MessageEvent('message', {
              data: {
                command: 'codegenModelsLoaded',
                models: fullCatalog,
              },
            }),
          );
        });

        const modelSelect = screen.getByTestId('codegen-model-select') as HTMLSelectElement;
        expect(modelSelect.value).toBe('gemini-4-hydrogen');

        // User manually selects gemini-3.8-flash, then switches to gemini-4-carbon
        fireEvent.change(modelSelect, { target: { value: 'gemini-3.8-flash' } });
        expect(modelSelect.value).toBe('gemini-3.8-flash');
        expect(screen.getByTestId('active-model-badge')).toHaveTextContent(
          'Model: gemini-3.8-flash',
        );

        fireEvent.change(modelSelect, { target: { value: 'gemini-4-carbon' } });
        expect(modelSelect.value).toBe('gemini-4-carbon');
        expect(screen.getByTestId('active-model-badge')).toHaveTextContent(
          'Model: gemini-4-carbon',
        );

        // User clicks Refresh models button
        mockPostMessage.mockClear();
        const refreshBtn = screen.getByTestId('refresh-codegen-models-btn');
        fireEvent.click(refreshBtn);
        expect(mockPostMessage).toHaveBeenCalledWith({ command: 'getCodegenModels' });

        // Refreshed catalog still contains gemini-4-carbon -> manual selection is preserved
        act(() => {
          window.dispatchEvent(
            new MessageEvent('message', {
              data: {
                command: 'codegenModelsLoaded',
                models: fullCatalog,
              },
            }),
          );
        });

        expect(modelSelect.value).toBe('gemini-4-carbon');
        expect(screen.getByTestId('active-model-badge')).toHaveTextContent(
          'Model: gemini-4-carbon',
        );

        fireEvent.click(screen.getByText('Generate Code'));
        expect(onRegenerate).toHaveBeenCalledWith({
          enableReviewer: false,
          model: 'gemini-4-carbon',
        });

        // If a subsequent refresh removes gemini-4-carbon, selector falls back to Flash default
        fireEvent.click(refreshBtn);
        act(() => {
          window.dispatchEvent(
            new MessageEvent('message', {
              data: {
                command: 'codegenModelsLoaded',
                models: fullCatalog.slice(0, 2),
              },
            }),
          );
        });

        expect(modelSelect.value).toBe('gemini-4-hydrogen');
        expect(screen.getByTestId('active-model-badge')).toHaveTextContent(
          'Model: gemini-4-hydrogen',
        );
      } finally {
        delete vsWindow.vscode;
      }
    });

    it('keeps offline fallback gemini-4-hydrogen active and renders inline warning when discovery fails', () => {
      const mockPostMessage = vi.fn();
      vsWindow.vscode = { postMessage: mockPostMessage };
      const onRegenerate = vi.fn();

      try {
        renderWithBottomPanel(<SourceCodeTab />, {
          ...mockProps,
          onRegenerate,
        });

        act(() => {
          window.dispatchEvent(
            new MessageEvent('message', {
              data: {
                command: 'codegenModelsLoaded',
                models: [],
                error: 'Vertex AI model listing unavailable offline',
              },
            }),
          );
        });

        const modelSelect = screen.getByTestId('codegen-model-select') as HTMLSelectElement;
        expect(modelSelect.value).toBe('gemini-4-hydrogen');
        expect(screen.getByTestId('active-model-badge')).toHaveTextContent(
          'Model: gemini-4-hydrogen',
        );
        expect(screen.getByTestId('codegen-models-error')).toHaveTextContent(
          'Vertex AI model listing unavailable offline',
        );

        fireEvent.click(screen.getByText('Generate Code'));
        expect(onRegenerate).toHaveBeenCalledWith({
          enableReviewer: false,
          model: 'gemini-4-hydrogen',
        });
      } finally {
        delete vsWindow.vscode;
      }
    });

    it('preserves a controlled selectedModel not yet in catalog options', () => {
      renderWithBottomPanel(<SourceCodeTab />, {
        ...mockProps,
        selectedModel: 'gemini-4-experimental',
        models: [
          {
            id: 'gemini-4-hydrogen',
            displayName: 'Gemini 4 Hydrogen',
            tier: 'flash',
            version: '4.0',
            isDefault: true,
          },
        ],
      });

      const modelSelect = screen.getByTestId('codegen-model-select') as HTMLSelectElement;
      expect(modelSelect.value).toBe('gemini-4-experimental');
      expect(screen.getByTestId('active-model-badge')).toHaveTextContent(
        'Model: gemini-4-experimental',
      );
    });
  });

  it('calls onSelectFile when clicking a file from the list', () => {
    renderWithBottomPanel(<SourceCodeTab />, mockProps);

    fireEvent.click(screen.getByText('job.py'));
    expect(mockProps.onSelectFile).toHaveBeenCalledWith('job.py');
  });

  it('renders rebuild warning and hides file list/code preview when needsRebuild is true', () => {
    renderWithBottomPanel(<SourceCodeTab />, { ...mockProps, needsRebuild: true });

    // Warning text should be visible
    expect(screen.getByText(/Unsaved Pipeline Changes/)).toBeInTheDocument();

    // File list should not show the files
    expect(screen.queryByText('job.py')).not.toBeInTheDocument();
    expect(screen.queryByText('requirements.txt')).not.toBeInTheDocument();

    // Code preview should not show the default code
    expect(screen.queryByText('print("pipeline")')).not.toBeInTheDocument();
  });

  it('renders selected file content when selectedFile is set', () => {
    renderWithBottomPanel(<SourceCodeTab />, {
      ...mockProps,
      files: ['job.py', 'custom_transform.py', 'requirements.txt'],
      selectedFile: 'custom_transform.py',
      selectedFileContent: 'def process(df): return df',
    });

    // Selected file tab button
    expect(screen.getByText('custom_transform.py', { selector: 'span' })).toBeInTheDocument();

    // Selected file content
    expect(
      screen.getByText((_content, node) => {
        return node?.tagName === 'CODE' && node?.textContent === 'def process(df): return df';
      }),
    ).toBeInTheDocument();
  });

  describe('highlightCode', () => {
    it('correctly highlights Java main method without token leakage', () => {
      const code = 'public static void main(String[] args) {\n    System.out.println("Hello");\n}';
      const highlighted = highlightCode(code, 'ParseMessagePipeline.java', 'Java');
      expect(highlighted).toContain('main');
      expect(highlighted).not.toContain('___TOKEN_');
      expect(highlighted).not.toContain('\uE000TOKEN_');
    });

    it('correctly highlights Python def main without token leakage', () => {
      const code = 'def main():\n    print("Hello")';
      const highlighted = highlightCode(code, 'pipeline.py', 'Python');
      expect(highlighted).toContain('main');
      expect(highlighted).not.toContain('___TOKEN_');
      expect(highlighted).not.toContain('\uE000TOKEN_');
    });

    it('renders replacement patterns ($&, $`, $\', $1) literally without expansion or corruption', () => {
      const code = 'cost = "$& tax"\nprefix = "$` before"\nsuffix = "$\' after"\ngroup = "$1 match"';
      const highlighted = highlightCode(code, 'pipeline.py', 'Python');
      expect(highlighted).toContain('$&amp; tax');
      expect(highlighted).toContain('$` before');
      expect(highlighted).toContain("$' after");
      expect(highlighted).toContain('$1 match');
      expect(highlighted).not.toContain('\uE000TOKEN_');
      expect(highlighted).not.toContain('___TOKEN_');
    });

    it('escapes HTML tags to prevent XSS injection from untrusted model code', () => {
      const maliciousCode = '<script>alert(1)</script>\n<img src=x onerror=1>';
      const highlighted = highlightCode(maliciousCode, 'pipeline.py', 'Python');

      expect(highlighted).not.toContain('<script>');
      expect(highlighted).not.toContain('</script>');
      expect(highlighted).not.toContain('<img');
      expect(highlighted).toContain('&lt;script&gt;');
      expect(highlighted).toContain('&lt;/script&gt;');
      expect(highlighted).toContain('&lt;img src=x onerror=1&gt;');
    });

    it('preserves expected syntax highlighting spans for representative Python snippets', () => {
      const pythonSnippet =
        '# Pipeline definition\nfrom pyspark.sql import SparkSession\n\ndef run():\n    msg = "Hello World"\n    print(msg)';
      const highlighted = highlightCode(pythonSnippet, 'pipeline.py', 'Python');

      // Comments
      expect(highlighted).toContain('<span class="text-slate-500"># Pipeline definition</span>');
      // Keywords
      expect(highlighted).toContain('<span class="text-pink-500 font-bold">from</span>');
      expect(highlighted).toContain('<span class="text-pink-500 font-bold">import</span>');
      expect(highlighted).toContain('<span class="text-pink-500 font-bold">def</span>');
      // Strings
      expect(highlighted).toContain('<span class="text-green-400">"Hello World"</span>');
      // Functions
      expect(highlighted).toContain('<span class="text-blue-400">run</span>');
      expect(highlighted).toContain('<span class="text-blue-400">print</span>');
    });

    it('preserves expected syntax highlighting spans for representative Java snippets', () => {
      const javaSnippet =
        '// Main pipeline class\npackage com.example;\n\npublic class Pipeline {\n    /* entrypoint */\n    public static void main(String[] args) {\n        System.out.println("Started");\n    }\n}';
      const highlighted = highlightCode(javaSnippet, 'Pipeline.java', 'Java');

      // Comments
      expect(highlighted).toContain('<span class="text-slate-500">// Main pipeline class</span>');
      expect(highlighted).toContain('<span class="text-slate-500">/* entrypoint */</span>');
      // Keywords
      expect(highlighted).toContain('<span class="text-pink-500 font-bold">package</span>');
      expect(highlighted).toContain('<span class="text-pink-500 font-bold">public</span>');
      expect(highlighted).toContain('<span class="text-pink-500 font-bold">class</span>');
      expect(highlighted).toContain('<span class="text-pink-500 font-bold">static</span>');
      expect(highlighted).toContain('<span class="text-pink-500 font-bold">void</span>');
      expect(highlighted).toContain('<span class="text-pink-500 font-bold">main</span>');
      expect(highlighted).toContain('<span class="text-pink-500 font-bold">String</span>');
      // Strings
      expect(highlighted).toContain('<span class="text-green-400">"Started"</span>');
      // Methods
      expect(highlighted).toContain('<span class="text-blue-400">println</span>');
    });

    it('does not recompute highlightCode when re-rendering with identical props', () => {
      const spy = vi.fn(highlightCode);
      const { rerenderWithContext } = renderWithBottomPanel(
        <SourceCodeTab highlighter={spy} />,
        mockProps,
      );
      expect(spy).toHaveBeenCalledTimes(1);

      // Re-rendering with an unrelated prop change does NOT re-run the highlighter
      rerenderWithContext(<SourceCodeTab highlighter={spy} />, {
        ...mockProps,
        outputFolder: 'new/output/folder',
      });
      expect(spy).toHaveBeenCalledTimes(1);

      // Re-rendering with changed code DOES re-run the highlighter
      rerenderWithContext(<SourceCodeTab highlighter={spy} />, {
        ...mockProps,
        generatedCode: "print('updated')",
      });
      expect(spy).toHaveBeenCalledTimes(2);
    });

    it('renders token usage message when tokenUsage prop is provided', () => {
      renderWithBottomPanel(<SourceCodeTab />, {
        ...mockProps,
        tokenUsage: {
          inputTokens: 1420,
          outputTokens: 856,
          totalTokens: 2276,
        },
      });

      const tokenBox = screen.getByTestId('token-usage');
      expect(tokenBox).toBeInTheDocument();
      expect(screen.getByText('Tokens:')).toBeInTheDocument();
      expect(screen.getByText('1,420')).toBeInTheDocument();
      expect(screen.getByText('856')).toBeInTheDocument();
    });

    it('does not render token usage message when tokenUsage is null or undefined', () => {
      renderWithBottomPanel(<SourceCodeTab />, { ...mockProps, tokenUsage: null });
      expect(screen.queryByTestId('token-usage')).not.toBeInTheDocument();
    });

    it('disables Generate Code button and displays validation error callout when isGraphValid is false', () => {
      renderWithBottomPanel(<SourceCodeTab />, {
        ...mockProps,
        isGraphValid: false,
        validationErrors: ['Cyclic dependency detected: Step 1 -> Step 2 -> Step 1'],
      });

      const generateBtn = screen.getByText('Generate Code').closest('button');
      expect(generateBtn).toBeDisabled();

      const errorCallout = screen.getByTestId('codegen-validation-error');
      expect(errorCallout).toBeInTheDocument();
      expect(errorCallout).toHaveTextContent('Cannot Generate Code');
      expect(errorCallout).toHaveTextContent(
        'Cyclic dependency detected: Step 1 -> Step 2 -> Step 1',
      );

      // Clicking disabled button does not trigger onRegenerate
      fireEvent.click(generateBtn!);
      expect(mockProps.onRegenerate).not.toHaveBeenCalled();
      expect(generateBtn).toHaveAttribute(
        'title',
        'Cannot Generate Code:\nCyclic dependency detected: Step 1 -> Step 2 -> Step 1',
      );
      expect(
        screen.getByText('Generation disabled: resolve diagram errors to continue'),
      ).toBeInTheDocument();
    });

    it('displays multiple validation errors and formats them as a list with tooltip on button', () => {
      renderWithBottomPanel(<SourceCodeTab />, {
        ...mockProps,
        isGraphValid: false,
        validationErrors: [
          'Cyclic dependency detected: Step 1 -> Step 2 -> Step 1',
          'Orphan node(s) detected: "Unconnected" (orphan). All steps must be connected into the pipeline.',
        ],
      });

      const generateBtn = screen.getByText('Generate Code').closest('button');
      expect(generateBtn).toBeDisabled();
      expect(generateBtn).toHaveAttribute(
        'title',
        'Cannot Generate Code:\nCyclic dependency detected: Step 1 -> Step 2 -> Step 1\nOrphan node(s) detected: "Unconnected" (orphan). All steps must be connected into the pipeline.',
      );

      const errorCallout = screen.getByTestId('codegen-validation-error');
      expect(errorCallout).toHaveTextContent(
        'Cyclic dependency detected: Step 1 -> Step 2 -> Step 1',
      );
      expect(errorCallout).toHaveTextContent('Orphan node(s) detected: "Unconnected" (orphan)');
      expect(
        screen.getByText('Generation disabled: resolve diagram errors to continue'),
      ).toBeInTheDocument();
    });

    it('enables Generate Code button and hides error callout when isGraphValid is true', () => {
      renderWithBottomPanel(<SourceCodeTab />, {
        ...mockProps,
        isGraphValid: true,
        validationErrors: [],
      });

      const generateBtn = screen.getByText('Generate Code').closest('button');
      expect(generateBtn).not.toBeDisabled();
      expect(generateBtn).not.toHaveAttribute('title');
      expect(screen.queryByTestId('codegen-validation-error')).not.toBeInTheDocument();
      expect(
        screen.queryByText('Generation disabled: resolve diagram errors to continue'),
      ).not.toBeInTheDocument();
    });

    it('renders code containing malicious HTML tags as literal text without creating DOM elements (XSS hardening)', () => {
      const maliciousSnippet = 'payload = "<img src=x onerror=alert(1)>"';
      const { container } = renderWithBottomPanel(<SourceCodeTab />, {
        ...mockProps,
        generatedCode: maliciousSnippet,
      });

      // Verify no img element is injected into the DOM
      expect(container.querySelector('img')).toBeNull();

      // Verify literal text is rendered visibly inside the code preview
      expect(
        screen.getByText((_content, node) => {
          return node?.tagName === 'CODE' && node?.textContent === maliciousSnippet;
        }),
      ).toBeInTheDocument();
    });
  });
});
