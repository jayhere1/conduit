import * as vscode from 'vscode';
import { compileDAGs } from '../commands/compile';

/**
 * Diagnostics provider that auto-compiles DAGs on file save/change
 * and publishes errors as VS Code diagnostics (red/yellow squiggles).
 */
export class ConduitDiagnosticsProvider implements vscode.Disposable {
    private disposables: vscode.Disposable[] = [];
    private debounceTimer: ReturnType<typeof setTimeout> | null = null;
    private outputChannel: vscode.OutputChannel;
    private statusBarItem: vscode.StatusBarItem;

    constructor(
        outputChannel: vscode.OutputChannel,
        statusBarItem: vscode.StatusBarItem,
        context: vscode.ExtensionContext
    ) {
        this.outputChannel = outputChannel;
        this.statusBarItem = statusBarItem;

        // Compile on save
        this.disposables.push(
            vscode.workspace.onDidSaveTextDocument((document) => {
                if (this.shouldCompile(document)) {
                    this.debouncedCompile();
                }
            })
        );

        // Compile on text change (only if the document is a Python file)
        this.disposables.push(
            vscode.workspace.onDidChangeTextDocument((event) => {
                if (this.shouldCompile(event.document)) {
                    this.debouncedCompile();
                }
            })
        );
    }

    /**
     * Check if the document is a Python file that could contain DAGs.
     */
    private shouldCompile(document: vscode.TextDocument): boolean {
        const config = vscode.workspace.getConfiguration('conduit');
        if (!config.get<boolean>('compileOnSave', true)) {
            return false;
        }

        return document.languageId === 'python' && document.uri.scheme === 'file';
    }

    /**
     * Debounced compile to avoid thrashing on rapid edits.
     */
    private debouncedCompile(): void {
        if (this.debounceTimer) {
            clearTimeout(this.debounceTimer);
        }

        const config = vscode.workspace.getConfiguration('conduit');
        const debounceMs = config.get<number>('compileDebounceMs', 300);

        this.debounceTimer = setTimeout(() => {
            this.debounceTimer = null;
            compileDAGs(this.outputChannel, this.statusBarItem, false);
        }, debounceMs);
    }

    public dispose(): void {
        if (this.debounceTimer) {
            clearTimeout(this.debounceTimer);
        }
        for (const d of this.disposables) {
            d.dispose();
        }
    }
}
