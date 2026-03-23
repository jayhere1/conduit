import * as vscode from 'vscode';
import * as path from 'path';
import {
    getDagsPath,
    runConduitCommand,
    parseCompileOutput,
    CompileResult,
    CompileError,
} from '../utils';

let diagnosticCollection: vscode.DiagnosticCollection;

export function initCompileDiagnostics(context: vscode.ExtensionContext): vscode.DiagnosticCollection {
    diagnosticCollection = vscode.languages.createDiagnosticCollection('conduit');
    context.subscriptions.push(diagnosticCollection);
    return diagnosticCollection;
}

export function getCompileDiagnostics(): vscode.DiagnosticCollection {
    return diagnosticCollection;
}

/**
 * Run conduit compile and return the parsed result.
 * Also publishes diagnostics and updates the status bar.
 */
export async function compileDAGs(
    outputChannel: vscode.OutputChannel,
    statusBarItem: vscode.StatusBarItem,
    showNotification: boolean = true
): Promise<CompileResult | null> {
    const dagsPath = getDagsPath();

    statusBarItem.text = '$(sync~spin) Conduit: Compiling...';
    statusBarItem.tooltip = 'Compiling DAGs...';
    statusBarItem.show();

    outputChannel.appendLine(`[Compile] Running: conduit compile ${dagsPath}`);
    outputChannel.appendLine('');

    const result = await runConduitCommand(['compile', dagsPath]);
    const parsed = parseCompileOutput(result.stdout, result.stderr);

    // Show raw output in channel
    if (result.stdout) {
        outputChannel.appendLine(result.stdout);
    }
    if (result.stderr) {
        outputChannel.appendLine(result.stderr);
    }

    // Clear previous diagnostics
    diagnosticCollection.clear();

    if (parsed.success) {
        statusBarItem.text = `$(check) Conduit: ${parsed.dagsCompiled} DAGs, ${parsed.totalTasks} tasks`;
        statusBarItem.tooltip = `Compiled ${parsed.dagsCompiled} DAGs with ${parsed.totalTasks} tasks in ${parsed.duration}\nClick to compile again`;
        statusBarItem.backgroundColor = undefined;

        if (showNotification) {
            vscode.window.showInformationMessage(
                `Conduit: Compiled ${parsed.dagsCompiled} DAGs (${parsed.totalTasks} tasks) in ${parsed.duration}`
            );
        }
    } else {
        statusBarItem.text = `$(error) Conduit: ${parsed.errors} error(s)`;
        statusBarItem.tooltip = `Compilation failed with ${parsed.errors} error(s)\nClick to compile again`;
        statusBarItem.backgroundColor = new vscode.ThemeColor('statusBarItem.errorBackground');

        // Publish diagnostics
        publishDiagnostics(parsed.rawErrors, dagsPath);

        if (showNotification) {
            const action = await vscode.window.showErrorMessage(
                `Conduit: Compilation failed with ${parsed.errors} error(s)`,
                'Show Output'
            );
            if (action === 'Show Output') {
                outputChannel.show();
            }
        }
    }

    return parsed;
}

/**
 * Convert compile errors to VS Code diagnostics.
 */
function publishDiagnostics(errors: CompileError[], dagsPath: string): void {
    const diagnosticsMap = new Map<string, vscode.Diagnostic[]>();

    for (const error of errors) {
        // Resolve the file path
        let filePath = error.file;
        if (!path.isAbsolute(filePath)) {
            filePath = path.join(dagsPath, filePath);
        }

        const uri = vscode.Uri.file(filePath);
        const key = uri.toString();

        if (!diagnosticsMap.has(key)) {
            diagnosticsMap.set(key, []);
        }

        const line = Math.max(0, error.line - 1); // VS Code lines are 0-indexed
        const col = Math.max(0, error.column);

        const range = new vscode.Range(line, col, line, Number.MAX_SAFE_INTEGER);
        const severity = error.severity === 'error'
            ? vscode.DiagnosticSeverity.Error
            : vscode.DiagnosticSeverity.Warning;

        const diagnostic = new vscode.Diagnostic(range, error.message, severity);
        diagnostic.source = 'conduit';

        diagnosticsMap.get(key)!.push(diagnostic);
    }

    for (const [uriString, diags] of diagnosticsMap) {
        diagnosticCollection.set(vscode.Uri.parse(uriString), diags);
    }
}
