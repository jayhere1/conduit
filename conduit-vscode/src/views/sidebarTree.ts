import * as vscode from 'vscode';
import {
    getDagsPath,
    runConduitCommand,
    parseCompileOutput,
    DagInfo,
    TaskInfo,
} from '../utils';

// ── Tree item types ─────────────────────────────────────────────────────────

type TreeItem = DagTreeItem | TaskTreeItem | SectionItem | ActionItem;

class SectionItem extends vscode.TreeItem {
    constructor(
        public readonly label: string,
        public readonly section: 'dags' | 'environments' | 'actions',
        collapsibleState: vscode.TreeItemCollapsibleState = vscode.TreeItemCollapsibleState.Expanded,
    ) {
        super(label, collapsibleState);
        this.contextValue = 'section';
        switch (section) {
            case 'dags':
                this.iconPath = new vscode.ThemeIcon('git-merge');
                break;
            case 'environments':
                this.iconPath = new vscode.ThemeIcon('layers');
                break;
            case 'actions':
                this.iconPath = new vscode.ThemeIcon('zap');
                break;
        }
    }
}

class DagTreeItem extends vscode.TreeItem {
    constructor(public readonly dag: DagInfo) {
        super(dag.id, vscode.TreeItemCollapsibleState.Collapsed);
        this.contextValue = 'dag';
        this.description = `${dag.taskCount} tasks`;
        this.tooltip = new vscode.MarkdownString(
            `**${dag.id}**\n\n` +
            `Schedule: \`${dag.schedule}\`\n\n` +
            `Tasks: ${dag.taskCount}\n\n` +
            dag.tasks.map(t => `- ${t.name}${t.isRoot ? ' _(root)_' : ''}`).join('\n')
        );
        this.iconPath = new vscode.ThemeIcon('git-merge', new vscode.ThemeColor('charts.green'));
    }
}

class TaskTreeItem extends vscode.TreeItem {
    constructor(
        public readonly task: TaskInfo,
        public readonly dagId: string,
    ) {
        super(task.name, vscode.TreeItemCollapsibleState.None);
        this.contextValue = 'task';

        if (task.isRoot) {
            this.description = 'root';
            this.iconPath = new vscode.ThemeIcon('circle-filled', new vscode.ThemeColor('charts.green'));
        } else {
            this.description = task.dependencies.join(', ');
            this.iconPath = new vscode.ThemeIcon('circle-outline', new vscode.ThemeColor('charts.blue'));
        }

        this.tooltip = new vscode.MarkdownString(
            `**${task.name}**\n\n` +
            (task.isRoot ? 'Root task (no dependencies)\n\n' : `Dependencies: ${task.dependencies.join(', ')}\n\n`) +
            `DAG: ${dagId}`
        );

        // Click to open the source file and search for this function
        this.command = {
            command: 'workbench.action.quickOpen',
            title: 'Go to task',
            arguments: [`def ${task.name}(`],
        };
    }
}

class ActionItem extends vscode.TreeItem {
    constructor(
        label: string,
        public readonly actionCommand: string,
        icon: string,
        args?: unknown[],
    ) {
        super(label, vscode.TreeItemCollapsibleState.None);
        this.contextValue = 'action';
        this.iconPath = new vscode.ThemeIcon(icon);
        this.command = {
            command: actionCommand,
            title: label,
            arguments: args,
        };
    }
}

// ── Tree Data Provider ──────────────────────────────────────────────────────

export class ConduitTreeProvider implements vscode.TreeDataProvider<TreeItem> {
    private _onDidChangeTreeData = new vscode.EventEmitter<TreeItem | undefined | null>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    private dags: DagInfo[] = [];
    private compileError: string | null = null;
    private loading = false;

    constructor() {
        this.refresh();
    }

    async refresh(): Promise<void> {
        if (this.loading) return;
        this.loading = true;

        const dagsPath = getDagsPath();
        const result = await runConduitCommand(['compile', dagsPath]);
        const parsed = parseCompileOutput(result.stdout, result.stderr);

        this.dags = parsed.dags;
        this.compileError = parsed.success ? null : result.stderr || 'Compilation failed';
        this.loading = false;
        this._onDidChangeTreeData.fire(undefined);
    }

    getTreeItem(element: TreeItem): vscode.TreeItem {
        return element;
    }

    getChildren(element?: TreeItem): TreeItem[] {
        if (!element) {
            // Root level: sections
            return [
                new SectionItem(`DAGs (${this.dags.length})`, 'dags'),
                new SectionItem('Quick Actions', 'actions'),
            ];
        }

        if (element instanceof SectionItem) {
            if (element.section === 'dags') {
                if (this.compileError) {
                    const errItem = new vscode.TreeItem('Compile error') as ActionItem;
                    errItem.iconPath = new vscode.ThemeIcon('error', new vscode.ThemeColor('errorForeground'));
                    errItem.description = 'Click to see details';
                    errItem.command = { command: 'conduit.compile', title: 'Compile' };
                    return [errItem as unknown as TreeItem];
                }
                return this.dags.map(dag => new DagTreeItem(dag));
            }
            if (element.section === 'actions') {
                return [
                    new ActionItem('Compile DAGs', 'conduit.compile', 'gear'),
                    new ActionItem('Run DAG...', 'conduit.run', 'play'),
                    new ActionItem('Show Graph...', 'conduit.showGraph', 'type-hierarchy'),
                    new ActionItem('Deployment Plan...', 'conduit.plan', 'diff'),
                ];
            }
        }

        if (element instanceof DagTreeItem) {
            return element.dag.tasks.map(task => new TaskTreeItem(task, element.dag.id));
        }

        return [];
    }
}
