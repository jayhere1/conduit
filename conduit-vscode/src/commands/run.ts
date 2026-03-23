import * as vscode from 'vscode';
import {
    getDagsPath,
    runConduitCommand,
    runConduitCommandStreaming,
    parseCompileOutput,
} from '../utils';

/**
 * Run a DAG by selecting from the compiled DAG list.
 */
export async function runDAG(
    outputChannel: vscode.OutputChannel,
    dagId?: string
): Promise<void> {
    const dagsPath = getDagsPath();

    // If no dagId supplied, compile first to discover available DAGs, then present a picker
    if (!dagId) {
        const result = await vscode.window.withProgress(
            {
                location: vscode.ProgressLocation.Notification,
                title: 'Conduit: Discovering DAGs...',
                cancellable: false,
            },
            async () => {
                return runConduitCommand(['compile', dagsPath]);
            }
        );

        const parsed = parseCompileOutput(result.stdout, result.stderr);

        if (parsed.dags.length === 0) {
            vscode.window.showWarningMessage('Conduit: No DAGs found. Check your DAGs path.');
            return;
        }

        const items: vscode.QuickPickItem[] = parsed.dags.map((dag) => ({
            label: dag.id,
            description: `${dag.taskCount} tasks`,
            detail: `Schedule: ${dag.schedule}`,
        }));

        const selected = await vscode.window.showQuickPick(items, {
            placeHolder: 'Select a DAG to run',
            title: 'Conduit: Run DAG',
        });

        if (!selected) {
            return; // User cancelled
        }

        dagId = selected.label;
    }

    // Show output channel and run the DAG
    outputChannel.show(true);
    outputChannel.appendLine('='.repeat(60));
    outputChannel.appendLine(`[Run] Starting DAG: ${dagId}`);
    outputChannel.appendLine(`[Run] DAGs path: ${dagsPath}`);
    outputChannel.appendLine(`[Run] Time: ${new Date().toISOString()}`);
    outputChannel.appendLine('='.repeat(60));
    outputChannel.appendLine('');

    const exitCode = await vscode.window.withProgress(
        {
            location: vscode.ProgressLocation.Notification,
            title: `Conduit: Running ${dagId}...`,
            cancellable: false,
        },
        async () => {
            return runConduitCommandStreaming(
                ['run', dagId!, '--dags-path', dagsPath],
                outputChannel
            );
        }
    );

    outputChannel.appendLine('');
    outputChannel.appendLine('='.repeat(60));

    if (exitCode === 0) {
        vscode.window.showInformationMessage(`Conduit: DAG '${dagId}' completed successfully.`);
    } else {
        const action = await vscode.window.showErrorMessage(
            `Conduit: DAG '${dagId}' failed (exit code ${exitCode}).`,
            'Show Output'
        );
        if (action === 'Show Output') {
            outputChannel.show();
        }
    }
}
