import * as vscode from 'vscode';
import { compileDAGs, initCompileDiagnostics } from './commands/compile';
import { runDAG } from './commands/run';
import { showPlan } from './commands/plan';
import { ConduitCodeLensProvider } from './providers/codeLens';
import { ConduitDiagnosticsProvider } from './providers/diagnostics';
import { registerDecorations, invalidateDecorationCache, updateDecorations } from './providers/decorations';
import { showDagGraph, showTaskLineage } from './views/dagGraph';
import { ConduitTreeProvider } from './views/sidebarTree';

let outputChannel: vscode.OutputChannel;
let statusBarItem: vscode.StatusBarItem;

export function activate(context: vscode.ExtensionContext): void {
    // -- Output channel --
    outputChannel = vscode.window.createOutputChannel('Conduit');
    context.subscriptions.push(outputChannel);

    // -- Status bar item --
    statusBarItem = vscode.window.createStatusBarItem(
        vscode.StatusBarAlignment.Left,
        50
    );
    statusBarItem.text = '$(circuit-board) Conduit';
    statusBarItem.tooltip = 'Conduit Pipeline Orchestrator - Click to compile';
    statusBarItem.command = 'conduit.compile';
    statusBarItem.show();
    context.subscriptions.push(statusBarItem);

    // -- Diagnostics collection --
    initCompileDiagnostics(context);

    // -- Sidebar tree view --
    const treeProvider = new ConduitTreeProvider();
    vscode.window.registerTreeDataProvider('conduit.dagTree', treeProvider);

    // -- Commands --
    context.subscriptions.push(
        vscode.commands.registerCommand('conduit.compile', async () => {
            await compileDAGs(outputChannel, statusBarItem);
            treeProvider.refresh();
            invalidateDecorationCache();
            if (vscode.window.activeTextEditor) {
                updateDecorations(vscode.window.activeTextEditor);
            }
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('conduit.run', (dagIdOrItem?: string | { dag?: { id: string } }) => {
            // Handle both string dagId and tree item with .dag.id
            let dagId: string | undefined;
            if (typeof dagIdOrItem === 'string') {
                dagId = dagIdOrItem;
            } else if (dagIdOrItem && typeof dagIdOrItem === 'object' && 'dag' in dagIdOrItem) {
                dagId = dagIdOrItem.dag?.id;
            }
            return runDAG(outputChannel, dagId);
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('conduit.plan', () => {
            return showPlan(outputChannel);
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('conduit.showGraph', (dagIdOrItem?: string | { dag?: { id: string } }) => {
            let dagId: string | undefined;
            if (typeof dagIdOrItem === 'string') {
                dagId = dagIdOrItem;
            } else if (dagIdOrItem && typeof dagIdOrItem === 'object' && 'dag' in dagIdOrItem) {
                dagId = dagIdOrItem.dag?.id;
            }
            return showDagGraph(context, dagId);
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand(
            'conduit.showLineage',
            (taskNameOrItem?: string | { task?: { name: string } }, sourceUri?: vscode.Uri) => {
                let taskName: string | undefined;
                if (typeof taskNameOrItem === 'string') {
                    taskName = taskNameOrItem;
                } else if (taskNameOrItem && typeof taskNameOrItem === 'object' && 'task' in taskNameOrItem) {
                    taskName = taskNameOrItem.task?.name;
                }
                return showTaskLineage(context, taskName, sourceUri);
            }
        )
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('conduit.refreshTree', () => {
            treeProvider.refresh();
        })
    );

    // -- CodeLens provider --
    const codeLensProvider = new ConduitCodeLensProvider();
    context.subscriptions.push(
        vscode.languages.registerCodeLensProvider(
            { language: 'python', scheme: 'file' },
            codeLensProvider
        )
    );

    // -- Inline decorations (@dag → "4 tasks · 0 */4 * * *") --
    registerDecorations(context);

    // -- Diagnostics provider (auto-compile on save/change) --
    const diagnosticsProvider = new ConduitDiagnosticsProvider(
        outputChannel,
        statusBarItem,
        context
    );
    context.subscriptions.push(diagnosticsProvider);

    // -- File watcher to refresh CodeLenses and tree --
    const fileWatcher = vscode.workspace.createFileSystemWatcher('**/*.py');
    fileWatcher.onDidChange(() => {
        codeLensProvider.refresh();
        treeProvider.refresh();
    });
    fileWatcher.onDidCreate(() => {
        codeLensProvider.refresh();
        treeProvider.refresh();
    });
    fileWatcher.onDidDelete(() => {
        codeLensProvider.refresh();
        treeProvider.refresh();
    });
    context.subscriptions.push(fileWatcher);

    // -- Initial compile on activation --
    compileDAGs(outputChannel, statusBarItem, false);

    outputChannel.appendLine('Conduit extension activated.');
}

export function deactivate(): void {
    // Cleanup is handled by disposables registered on the context
}
