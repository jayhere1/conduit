import * as vscode from 'vscode';
import {
    getDagsPath,
    runConduitCommand,
    parseCompileOutput,
    DagInfo,
    TaskInfo,
} from '../utils';

const openPanels = new Map<string, vscode.WebviewPanel>();

/**
 * Show an interactive DAG graph in a webview panel.
 */
export async function showDagGraph(
    context: vscode.ExtensionContext,
    dagId?: string
): Promise<void> {
    const dagsPath = getDagsPath();

    const result = await vscode.window.withProgress(
        {
            location: vscode.ProgressLocation.Notification,
            title: 'Conduit: Loading DAG graph...',
            cancellable: false,
        },
        async () => {
            return runConduitCommand(['compile', dagsPath]);
        }
    );

    const parsed = parseCompileOutput(result.stdout, result.stderr);

    if (parsed.dags.length === 0) {
        vscode.window.showWarningMessage('Conduit: No DAGs found.');
        return;
    }

    if (!dagId) {
        const items: vscode.QuickPickItem[] = parsed.dags.map((dag) => ({
            label: dag.id,
            description: `${dag.taskCount} tasks`,
            detail: `Schedule: ${dag.schedule}`,
        }));

        const selected = await vscode.window.showQuickPick(items, {
            placeHolder: 'Select a DAG to visualize',
            title: 'Conduit: Show DAG Graph',
        });

        if (!selected) {
            return;
        }
        dagId = selected.label;
    }

    const dag = parsed.dags.find((d) => d.id === dagId);
    if (!dag) {
        vscode.window.showErrorMessage(`Conduit: DAG '${dagId}' not found.`);
        return;
    }

    const existingPanel = openPanels.get(dagId);
    if (existingPanel) {
        existingPanel.reveal();
        existingPanel.webview.html = generateGraphHtml(dag);
        return;
    }

    const panel = vscode.window.createWebviewPanel(
        'conduitDagGraph',
        `DAG: ${dagId}`,
        vscode.ViewColumn.Beside,
        {
            enableScripts: true,
            retainContextWhenHidden: true,
        }
    );

    panel.webview.html = generateGraphHtml(dag);
    openPanels.set(dagId, panel);

    panel.onDidDispose(() => {
        openPanels.delete(dagId!);
    });

    // Watch for file changes and refresh
    const watcher = vscode.workspace.createFileSystemWatcher('**/*.py');
    const refreshDebounce = debounce(async () => {
        const freshResult = await runConduitCommand(['compile', dagsPath]);
        const freshParsed = parseCompileOutput(freshResult.stdout, freshResult.stderr);
        const freshDag = freshParsed.dags.find((d) => d.id === dagId);
        if (freshDag) {
            panel.webview.html = generateGraphHtml(freshDag);
        }
    }, 500);

    watcher.onDidChange(refreshDebounce);
    watcher.onDidCreate(refreshDebounce);

    panel.onDidDispose(() => {
        watcher.dispose();
    });
}

/**
 * Show lineage for a specific task.
 */
export async function showTaskLineage(
    context: vscode.ExtensionContext,
    taskName?: string,
    _sourceUri?: vscode.Uri
): Promise<void> {
    const dagsPath = getDagsPath();

    const result = await runConduitCommand(['compile', dagsPath]);
    const parsed = parseCompileOutput(result.stdout, result.stderr);

    const matchingDags: { dag: DagInfo; task: TaskInfo }[] = [];
    for (const dag of parsed.dags) {
        for (const task of dag.tasks) {
            if (task.name === taskName) {
                matchingDags.push({ dag, task });
            }
        }
    }

    if (matchingDags.length === 0) {
        vscode.window.showWarningMessage(
            `Conduit: Task '${taskName}' not found in any compiled DAG.`
        );
        return;
    }

    let selectedDag: DagInfo;
    if (matchingDags.length === 1) {
        selectedDag = matchingDags[0].dag;
    } else {
        const items = matchingDags.map((m) => ({
            label: m.dag.id,
            description: `Contains task '${taskName}'`,
        }));
        const selected = await vscode.window.showQuickPick(items, {
            placeHolder: 'This task appears in multiple DAGs. Select one.',
        });
        if (!selected) {
            return;
        }
        selectedDag = matchingDags.find((m) => m.dag.id === selected.label)!.dag;
    }

    const panel = vscode.window.createWebviewPanel(
        'conduitTaskLineage',
        `Lineage: ${taskName}`,
        vscode.ViewColumn.Beside,
        {
            enableScripts: true,
            retainContextWhenHidden: true,
        }
    );

    panel.webview.html = generateGraphHtml(selectedDag, taskName);
}

// ── Graph layout engine ─────────────────────────────────────────────────────

interface NodePos {
    x: number;
    y: number;
    layer: number;
}

function assignLayers(dag: DagInfo): Map<string, number> {
    const layers = new Map<string, number>();
    const taskMap = new Map<string, TaskInfo>();
    for (const task of dag.tasks) {
        taskMap.set(task.name, task);
    }

    function getLayer(name: string): number {
        if (layers.has(name)) {
            return layers.get(name)!;
        }
        const task = taskMap.get(name);
        if (!task || task.dependencies.length === 0) {
            layers.set(name, 0);
            return 0;
        }
        let maxDepLayer = 0;
        for (const dep of task.dependencies) {
            maxDepLayer = Math.max(maxDepLayer, getLayer(dep) + 1);
        }
        layers.set(name, maxDepLayer);
        return maxDepLayer;
    }

    for (const task of dag.tasks) {
        getLayer(task.name);
    }

    return layers;
}

// ── HTML generation (vertical top-down layout) ──────────────────────────────

function generateGraphHtml(dag: DagInfo, highlightTask?: string): string {
    const layers = assignLayers(dag);
    const maxLayer = Math.max(...Array.from(layers.values()), 0);

    // Group tasks by layer
    const layerGroups = new Map<number, TaskInfo[]>();
    for (const task of dag.tasks) {
        const layer = layers.get(task.name) ?? 0;
        if (!layerGroups.has(layer)) {
            layerGroups.set(layer, []);
        }
        layerGroups.get(layer)!.push(task);
    }

    // Vertical top-down layout
    const nodeW = 200;
    const nodeH = 56;
    const layerGap = 100; // vertical gap between layers
    const nodeGap = 40;   // horizontal gap between nodes in same layer
    const padX = 40;
    const padY = 40;

    // Find the widest layer
    let maxNodesInLayer = 0;
    for (const tasks of layerGroups.values()) {
        maxNodesInLayer = Math.max(maxNodesInLayer, tasks.length);
    }

    const totalWidth = Math.max(
        padX * 2 + maxNodesInLayer * nodeW + (maxNodesInLayer - 1) * nodeGap,
        400
    );

    const positions = new Map<string, NodePos>();

    for (let layer = 0; layer <= maxLayer; layer++) {
        const tasks = layerGroups.get(layer) || [];
        const rowWidth = tasks.length * nodeW + (tasks.length - 1) * nodeGap;
        const startX = (totalWidth - rowWidth) / 2;
        const y = padY + layer * (nodeH + layerGap);

        tasks.forEach((task, index) => {
            positions.set(task.name, {
                x: startX + index * (nodeW + nodeGap),
                y,
                layer,
            });
        });
    }

    const svgWidth = totalWidth;
    const svgHeight = padY * 2 + (maxLayer + 1) * nodeH + maxLayer * layerGap;

    // Determine highlight set for lineage mode
    const hlSet = new Set<string>();
    const hlEdges = new Set<string>();
    if (highlightTask) {
        hlSet.add(highlightTask);
        // Trace upstream
        const taskMap = new Map(dag.tasks.map(t => [t.name, t]));
        const traceUp = (name: string) => {
            const t = taskMap.get(name);
            if (!t) return;
            for (const dep of t.dependencies) {
                hlEdges.add(`${dep}->${name}`);
                if (!hlSet.has(dep)) {
                    hlSet.add(dep);
                    traceUp(dep);
                }
            }
        };
        traceUp(highlightTask);
        // Trace downstream
        const traceDown = (name: string) => {
            for (const t of dag.tasks) {
                if (t.dependencies.includes(name)) {
                    hlEdges.add(`${name}->${t.name}`);
                    if (!hlSet.has(t.name)) {
                        hlSet.add(t.name);
                        traceDown(t.name);
                    }
                }
            }
        };
        traceDown(highlightTask);
    }

    // Build SVG
    let edgesSvg = '';
    let nodesSvg = '';

    // Edges (vertical: top to bottom)
    for (const task of dag.tasks) {
        const to = positions.get(task.name);
        if (!to) continue;

        for (const depName of task.dependencies) {
            const from = positions.get(depName);
            if (!from) continue;

            const x1 = from.x + nodeW / 2;
            const y1 = from.y + nodeH;
            const x2 = to.x + nodeW / 2;
            const y2 = to.y;

            const cpY = (y1 + y2) / 2;
            const edgeKey = `${depName}->${task.name}`;
            const isHl = highlightTask ? hlEdges.has(edgeKey) : false;
            const dimmed = highlightTask && !isHl;

            const color = isHl ? '#60a5fa' : dimmed ? '#333' : '#555';
            const width = isHl ? 2.5 : 1.5;
            const opacity = dimmed ? 0.3 : 1;

            edgesSvg += `<path d="M${x1},${y1} C${x1},${cpY} ${x2},${cpY} ${x2},${y2}" fill="none" stroke="${color}" stroke-width="${width}" opacity="${opacity}" marker-end="url(#arrow${isHl ? '-hl' : dimmed ? '-dim' : ''})"/>`;
        }
    }

    // Nodes
    for (const task of dag.tasks) {
        const pos = positions.get(task.name);
        if (!pos) continue;

        const isFocus = task.name === highlightTask;
        const isInLineage = hlSet.size === 0 || hlSet.has(task.name);
        const dimmed = hlSet.size > 0 && !isInLineage;

        // Node style based on state
        let fill: string, stroke: string, textColor: string, opacity: number;
        if (isFocus) {
            fill = '#1e3a5f';
            stroke = '#60a5fa';
            textColor = '#fff';
            opacity = 1;
        } else if (dimmed) {
            fill = '#1a1a1a';
            stroke = '#333';
            textColor = '#555';
            opacity = 0.4;
        } else {
            fill = task.isRoot ? '#1a2e1a' : '#1e1e2e';
            stroke = task.isRoot ? '#4ade80' : '#6366f1';
            textColor = '#e5e5e5';
            opacity = 1;
        }

        const label = task.name.length > 24 ? task.name.slice(0, 22) + '..' : task.name;
        const subLabel = task.isRoot ? 'root' : `${task.dependencies.length} dep${task.dependencies.length !== 1 ? 's' : ''}`;

        nodesSvg += `
            <g class="node" data-task="${esc(task.name)}" opacity="${opacity}">
                <rect x="${pos.x}" y="${pos.y}" width="${nodeW}" height="${nodeH}"
                      rx="10" ry="10" fill="${fill}" stroke="${stroke}" stroke-width="${isFocus ? 2.5 : 1.5}"/>
                <text x="${pos.x + nodeW / 2}" y="${pos.y + 24}" fill="${textColor}"
                      font-family="'SF Mono','Cascadia Code','Fira Code',monospace" font-size="13"
                      text-anchor="middle" font-weight="600">${esc(label)}</text>
                <text x="${pos.x + nodeW / 2}" y="${pos.y + 42}" fill="${dimmed ? '#444' : '#888'}"
                      font-family="system-ui,sans-serif" font-size="11"
                      text-anchor="middle">${esc(subLabel)}</text>
            </g>`;
    }

    const tasksJson = JSON.stringify(dag.tasks.map(t => ({
        name: t.name,
        deps: t.dependencies,
        isRoot: t.isRoot,
    })));

    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    background: #0d1117;
    color: #c9d1d9;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    overflow: auto;
}
.header {
    padding: 16px 20px;
    background: #161b22;
    border-bottom: 1px solid #21262d;
    display: flex;
    align-items: center;
    gap: 12px;
    position: sticky;
    top: 0;
    z-index: 10;
}
.header h1 {
    font-size: 15px;
    font-weight: 600;
    font-family: 'SF Mono','Cascadia Code','Fira Code',monospace;
    color: #e6edf3;
}
.pill {
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
}
.pill-blue { background: #1f6feb33; color: #58a6ff; border: 1px solid #1f6feb55; }
.pill-green { background: #23863533; color: #3fb950; border: 1px solid #23863555; }
.pill-amber { background: #9e6a0333; color: #d29922; border: 1px solid #9e6a0355; }
.graph-wrap {
    display: flex;
    justify-content: center;
    padding: 24px 16px;
    min-height: calc(100vh - 120px);
}
svg { display: block; }
.node { cursor: pointer; transition: opacity 0.15s; }
.node:hover rect { stroke-width: 3 !important; filter: brightness(1.3); }
.stats {
    padding: 12px 20px;
    background: #161b22;
    border-top: 1px solid #21262d;
    display: flex;
    gap: 24px;
    font-size: 12px;
    color: #8b949e;
}
.stats .stat-val { color: #c9d1d9; font-weight: 600; }
.tip {
    position: fixed;
    background: #1c2128;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 10px 14px;
    font-size: 12px;
    color: #c9d1d9;
    pointer-events: none;
    display: none;
    z-index: 100;
    max-width: 300px;
    line-height: 1.5;
    box-shadow: 0 8px 24px rgba(0,0,0,0.4);
}
.tip strong { color: #e6edf3; }
.tip .dep-label { color: #8b949e; }
.tip .dep-list { color: #58a6ff; }
</style>
</head>
<body>
<div class="header">
    <h1>${esc(dag.id)}</h1>
    <span class="pill pill-blue">${dag.taskCount} tasks</span>
    <span class="pill pill-green">${esc(dag.schedule)}</span>
    ${highlightTask ? `<span class="pill pill-amber">lineage: ${esc(highlightTask)}</span>` : ''}
</div>
<div class="graph-wrap">
    <svg width="${svgWidth}" height="${svgHeight}" xmlns="http://www.w3.org/2000/svg">
        <defs>
            <marker id="arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
                <polygon points="0 0, 8 3, 0 6" fill="#555"/>
            </marker>
            <marker id="arrow-hl" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
                <polygon points="0 0, 8 3, 0 6" fill="#60a5fa"/>
            </marker>
            <marker id="arrow-dim" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
                <polygon points="0 0, 8 3, 0 6" fill="#333"/>
            </marker>
        </defs>
        ${edgesSvg}
        ${nodesSvg}
    </svg>
</div>
<div class="stats">
    <span><span class="stat-val">${dag.taskCount}</span> tasks</span>
    <span><span class="stat-val">${dag.tasks.filter(t => t.isRoot).length}</span> roots</span>
    <span><span class="stat-val">${maxLayer + 1}</span> layers</span>
    <span><span class="stat-val">${dag.tasks.reduce((sum, t) => sum + t.dependencies.length, 0)}</span> edges</span>
</div>
<div class="tip" id="tip"></div>
<script>
const tip = document.getElementById('tip');
const tasks = ${tasksJson};
const taskMap = {};
tasks.forEach(t => taskMap[t.name] = t);

document.querySelectorAll('.node').forEach(node => {
    node.addEventListener('mouseenter', () => {
        const name = node.getAttribute('data-task');
        const task = taskMap[name];
        if (!task) return;

        let h = '<strong>' + name + '</strong>';
        if (task.isRoot) h += ' <span class="dep-label">(root)</span>';
        if (task.deps.length > 0) {
            h += '<br/><span class="dep-label">upstream:</span> <span class="dep-list">' + task.deps.join(', ') + '</span>';
        }
        const ds = tasks.filter(t => t.deps.includes(name)).map(t => t.name);
        if (ds.length > 0) {
            h += '<br/><span class="dep-label">downstream:</span> <span class="dep-list">' + ds.join(', ') + '</span>';
        }
        tip.innerHTML = h;
        tip.style.display = 'block';
    });
    node.addEventListener('mousemove', (e) => {
        tip.style.left = (e.clientX + 14) + 'px';
        tip.style.top = (e.clientY + 14) + 'px';
    });
    node.addEventListener('mouseleave', () => { tip.style.display = 'none'; });
});
</script>
</body>
</html>`;
}

function esc(str: string): string {
    return str
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function debounce<T extends (...args: unknown[]) => unknown>(
    fn: T,
    ms: number
): (...args: Parameters<T>) => void {
    let timer: ReturnType<typeof setTimeout> | null = null;
    return (...args: Parameters<T>) => {
        if (timer) { clearTimeout(timer); }
        timer = setTimeout(() => {
            timer = null;
            fn(...args);
        }, ms);
    };
}
