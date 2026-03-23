import * as vscode from 'vscode';

/**
 * CodeLens provider that detects @dag and @task decorators in Python files
 * and provides inline actions to run, graph, and compile DAGs.
 */
export class ConduitCodeLensProvider implements vscode.CodeLensProvider {
    private _onDidChangeCodeLenses = new vscode.EventEmitter<void>();
    public readonly onDidChangeCodeLenses = this._onDidChangeCodeLenses.event;

    private dagPattern = /^@dag\b/;
    private taskPattern = /^@task\b/;
    private functionPattern = /^def\s+(\w+)\s*\(/;

    public refresh(): void {
        this._onDidChangeCodeLenses.fire();
    }

    public provideCodeLenses(
        document: vscode.TextDocument,
        _token: vscode.CancellationToken
    ): vscode.CodeLens[] {
        const lenses: vscode.CodeLens[] = [];
        const text = document.getText();
        const lines = text.split('\n');

        for (let i = 0; i < lines.length; i++) {
            const trimmed = lines[i].trim();

            // Detect @dag decorator
            if (this.dagPattern.test(trimmed)) {
                const dagName = this.findNextFunctionName(lines, i);
                const range = new vscode.Range(i, 0, i, lines[i].length);

                if (dagName) {
                    // Run DAG lens
                    lenses.push(
                        new vscode.CodeLens(range, {
                            title: '\u25B6 Run',
                            command: 'conduit.run',
                            arguments: [dagName],
                            tooltip: `Run DAG: ${dagName}`,
                        })
                    );

                    // Graph lens
                    lenses.push(
                        new vscode.CodeLens(range, {
                            title: '\uD83D\uDCCA Graph',
                            command: 'conduit.showGraph',
                            arguments: [dagName],
                            tooltip: `Show graph for DAG: ${dagName}`,
                        })
                    );

                    // Compile lens
                    lenses.push(
                        new vscode.CodeLens(range, {
                            title: '\u26A1 Compile',
                            command: 'conduit.compile',
                            tooltip: 'Compile all DAGs',
                        })
                    );
                }
            }

            // Detect @task decorator
            if (this.taskPattern.test(trimmed)) {
                const taskName = this.findNextFunctionName(lines, i);
                const range = new vscode.Range(i, 0, i, lines[i].length);

                if (taskName) {
                    lenses.push(
                        new vscode.CodeLens(range, {
                            title: '\uD83D\uDD0D View Lineage',
                            command: 'conduit.showLineage',
                            arguments: [taskName, document.uri],
                            tooltip: `Show lineage for task: ${taskName}`,
                        })
                    );
                }
            }
        }

        return lenses;
    }

    /**
     * Look ahead from a decorator line to find the next function definition.
     */
    private findNextFunctionName(lines: string[], fromLine: number): string | null {
        // Scan up to 10 lines ahead (decorators can be stacked and have arguments spanning lines)
        for (let i = fromLine + 1; i < Math.min(fromLine + 10, lines.length); i++) {
            const match = lines[i].match(this.functionPattern);
            if (match) {
                return match[1];
            }
        }
        return null;
    }
}
