import * as vscode from 'vscode';
import {
    getDagsPath,
    runConduitCommand,
    parseCompileOutput,
    DagInfo,
} from '../utils';

const dagDecorationType = vscode.window.createTextEditorDecorationType({
    after: {
        margin: '0 0 0 16px',
        fontStyle: 'italic',
    },
    isWholeLine: true,
});

let cachedDags: DagInfo[] = [];
let cacheTime = 0;
const CACHE_TTL = 5000; // 5 seconds

async function getDags(): Promise<DagInfo[]> {
    if (Date.now() - cacheTime < CACHE_TTL && cachedDags.length > 0) {
        return cachedDags;
    }
    const dagsPath = getDagsPath();
    const result = await runConduitCommand(['compile', dagsPath]);
    const parsed = parseCompileOutput(result.stdout, result.stderr);
    cachedDags = parsed.dags;
    cacheTime = Date.now();
    return cachedDags;
}

export function invalidateDecorationCache(): void {
    cacheTime = 0;
}

/**
 * Update inline decorations for the given editor.
 * Shows task count and schedule next to @dag decorators.
 */
export async function updateDecorations(editor: vscode.TextEditor): Promise<void> {
    if (editor.document.languageId !== 'python') {
        return;
    }

    const text = editor.document.getText();
    const decorations: vscode.DecorationOptions[] = [];

    // Find @dag and @task decorators
    const dagRegex = /@dag\s*\(/g;
    const taskRegex = /@task\s*\(/g;
    const defRegex = /def\s+(\w+)\s*\(/g;

    const dags = await getDags();

    // Build a map of function name -> DAG info
    const dagFuncMap = new Map<string, DagInfo>();
    // Scan for def lines that follow @dag decorators
    const lines = text.split('\n');
    let inDagDecorator = false;
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (/@dag\s*\(/.test(line)) {
            inDagDecorator = true;
            continue;
        }
        if (inDagDecorator) {
            const defMatch = /def\s+(\w+)\s*\(/.exec(line);
            if (defMatch) {
                const funcName = defMatch[1];
                const dag = dags.find(d => d.id === funcName);
                if (dag) {
                    dagFuncMap.set(funcName, dag);
                }
                inDagDecorator = false;
            }
            // Skip lines that are part of the decorator args
            if (line.trim().startsWith(')')) {
                // Next non-empty line should be def
                continue;
            }
        }
    }

    // Add decorations for @dag lines
    let match: RegExpExecArray | null;
    dagRegex.lastIndex = 0;
    while ((match = dagRegex.exec(text)) !== null) {
        const pos = editor.document.positionAt(match.index);
        const line = pos.line;

        // Look ahead for the def line to find which DAG this is
        for (let i = line; i < Math.min(line + 10, editor.document.lineCount); i++) {
            const lineText = editor.document.lineAt(i).text;
            const defMatch = /def\s+(\w+)\s*\(/.exec(lineText);
            if (defMatch) {
                const dagInfo = dagFuncMap.get(defMatch[1]);
                if (dagInfo) {
                    decorations.push({
                        range: new vscode.Range(line, 0, line, editor.document.lineAt(line).text.length),
                        renderOptions: {
                            after: {
                                contentText: `  ● ${dagInfo.taskCount} tasks · ${dagInfo.schedule}`,
                                color: new vscode.ThemeColor('editorCodeLens.foreground'),
                                fontStyle: 'italic',
                                fontSize: '12px',
                            },
                        },
                    });
                }
                break;
            }
        }
    }

    // Add decorations for @task lines showing dependency count
    taskRegex.lastIndex = 0;
    while ((match = taskRegex.exec(text)) !== null) {
        const pos = editor.document.positionAt(match.index);
        const line = pos.line;

        // Look ahead for the def line
        for (let i = line; i < Math.min(line + 10, editor.document.lineCount); i++) {
            const lineText = editor.document.lineAt(i).text;
            const defMatch = /def\s+(\w+)\s*\(/.exec(lineText);
            if (defMatch) {
                const taskName = defMatch[1];
                // Find this task in any DAG
                for (const dag of dags) {
                    const taskInfo = dag.tasks.find(t => t.name === taskName);
                    if (taskInfo) {
                        const depText = taskInfo.isRoot
                            ? 'root'
                            : `← ${taskInfo.dependencies.join(', ')}`;
                        decorations.push({
                            range: new vscode.Range(line, 0, line, editor.document.lineAt(line).text.length),
                            renderOptions: {
                                after: {
                                    contentText: `  ${depText}`,
                                    color: new vscode.ThemeColor('editorCodeLens.foreground'),
                                    fontStyle: 'italic',
                                    fontSize: '11px',
                                },
                            },
                        });
                        break;
                    }
                }
                break;
            }
        }
    }

    editor.setDecorations(dagDecorationType, decorations);
}

/**
 * Set up decoration listeners.
 */
export function registerDecorations(context: vscode.ExtensionContext): void {
    // Update decorations when the active editor changes
    vscode.window.onDidChangeActiveTextEditor(
        (editor) => {
            if (editor) {
                updateDecorations(editor);
            }
        },
        null,
        context.subscriptions
    );

    // Update on document save
    vscode.workspace.onDidSaveTextDocument(
        (doc) => {
            if (doc.languageId === 'python') {
                invalidateDecorationCache();
                const editor = vscode.window.activeTextEditor;
                if (editor && editor.document === doc) {
                    updateDecorations(editor);
                }
            }
        },
        null,
        context.subscriptions
    );

    // Initial decoration
    if (vscode.window.activeTextEditor) {
        updateDecorations(vscode.window.activeTextEditor);
    }
}
