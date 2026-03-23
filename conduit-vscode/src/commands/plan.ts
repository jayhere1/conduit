import * as vscode from 'vscode';
import {
    getDagsPath,
    runConduitCommand,
} from '../utils';

/**
 * Run `conduit plan` and display results in a virtual document.
 */
export async function showPlan(
    outputChannel: vscode.OutputChannel
): Promise<void> {
    const dagsPath = getDagsPath();

    // Ask for environment
    const envInput = await vscode.window.showInputBox({
        placeHolder: 'production',
        prompt: 'Target environment for the deployment plan',
        value: 'production',
        title: 'Conduit: Deployment Plan',
    });

    if (envInput === undefined) {
        return; // User cancelled
    }

    const env = envInput.trim() || 'production';

    const result = await vscode.window.withProgress(
        {
            location: vscode.ProgressLocation.Notification,
            title: `Conduit: Computing plan for ${env}...`,
            cancellable: false,
        },
        async () => {
            return runConduitCommand(['plan', env, '--dags-path', dagsPath]);
        }
    );

    outputChannel.appendLine(`[Plan] Environment: ${env}`);
    outputChannel.appendLine(`[Plan] DAGs path: ${dagsPath}`);
    outputChannel.appendLine('');

    if (result.stdout) {
        outputChannel.appendLine(result.stdout);
    }
    if (result.stderr) {
        outputChannel.appendLine(result.stderr);
    }

    // Show the plan in a virtual document for a nicer read-only view
    const content = formatPlanOutput(result.stdout, result.stderr, env);

    const doc = await vscode.workspace.openTextDocument({
        content,
        language: 'markdown',
    });

    await vscode.window.showTextDocument(doc, {
        preview: true,
        viewColumn: vscode.ViewColumn.Beside,
    });

    if (result.exitCode !== 0) {
        vscode.window.showWarningMessage(
            `Conduit: Plan command exited with code ${result.exitCode}. Check the output for details.`
        );
    }
}

/**
 * Format the plan output as readable markdown.
 */
function formatPlanOutput(stdout: string, stderr: string, env: string): string {
    const lines: string[] = [];

    lines.push(`# Conduit Deployment Plan`);
    lines.push('');
    lines.push(`**Environment:** ${env}`);
    lines.push(`**Generated:** ${new Date().toISOString()}`);
    lines.push('');
    lines.push('---');
    lines.push('');

    if (stdout.trim()) {
        // Parse and format the plan output
        const planLines = stdout.split('\n');

        for (const line of planLines) {
            // Highlight additions
            if (line.startsWith('+') || line.includes('create') || line.includes('add')) {
                lines.push(`> **+** ${line.trim()}`);
            }
            // Highlight removals
            else if (line.startsWith('-') || line.includes('remove') || line.includes('delete')) {
                lines.push(`> **-** ${line.trim()}`);
            }
            // Highlight modifications
            else if (line.startsWith('~') || line.includes('modify') || line.includes('update')) {
                lines.push(`> **~** ${line.trim()}`);
            }
            // Plain lines
            else {
                lines.push(line);
            }
        }
    } else {
        lines.push('*No changes detected.*');
    }

    if (stderr.trim()) {
        lines.push('');
        lines.push('---');
        lines.push('');
        lines.push('## Warnings / Errors');
        lines.push('');
        lines.push('```');
        lines.push(stderr.trim());
        lines.push('```');
    }

    return lines.join('\n');
}
