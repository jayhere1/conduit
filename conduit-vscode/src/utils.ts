import * as vscode from 'vscode';
import * as cp from 'child_process';
import * as path from 'path';

export interface ConduitCommandResult {
    stdout: string;
    stderr: string;
    exitCode: number;
}

/**
 * Resolves the path to the conduit binary.
 * Uses the configured path, or falls back to 'conduit' on PATH.
 */
export function getConduitBinary(): string {
    const config = vscode.workspace.getConfiguration('conduit');
    const configured = config.get<string>('binaryPath', '');
    if (configured && configured.trim().length > 0) {
        return configured.trim();
    }

    // Try to find the binary relative to the workspace (release or debug build)
    const fs = require('fs');
    const workspaceFolders = vscode.workspace.workspaceFolders;
    if (workspaceFolders) {
        for (const folder of workspaceFolders) {
            const root = folder.uri.fsPath;
            // Check for conduit project with built binary
            const candidates = [
                path.join(root, 'target', 'release', 'conduit'),
                path.join(root, 'target', 'debug', 'conduit'),
                // If workspace is a parent directory containing conduit/
                path.join(root, 'conduit', 'target', 'release', 'conduit'),
                path.join(root, 'conduit', 'target', 'debug', 'conduit'),
            ];
            for (const candidate of candidates) {
                if (fs.existsSync(candidate)) {
                    return candidate;
                }
            }
        }
    }

    return 'conduit';
}

/**
 * Resolves the DAGs path from configuration or workspace root.
 * Checks for common DAG directory names in the workspace.
 */
export function getDagsPath(): string {
    const config = vscode.workspace.getConfiguration('conduit');
    const configured = config.get<string>('dagsPath', '');
    if (configured && configured.trim().length > 0) {
        return configured.trim();
    }

    const workspaceFolders = vscode.workspace.workspaceFolders;
    if (!workspaceFolders || workspaceFolders.length === 0) {
        return './dags';
    }

    const root = workspaceFolders[0].uri.fsPath;

    // Check common DAG directory names (also check inside conduit/ subdirectory)
    const candidates = [
        'dags', 'pipelines', 'workflows',
        path.join('conduit', 'dags'),
        path.join('conduit', 'pipelines'),
        path.join('conduit', 'workflows'),
    ];
    const fs = require('fs');
    for (const candidate of candidates) {
        const candidatePath = path.join(root, candidate);
        if (fs.existsSync(candidatePath)) {
            return candidatePath;
        }
    }

    // Fall back to workspace root
    return root;
}

/**
 * Returns the configured API URL.
 */
export function getApiUrl(): string {
    const config = vscode.workspace.getConfiguration('conduit');
    return config.get<string>('apiUrl', 'http://localhost:9091/api/v1');
}

/**
 * Builds environment variables for conduit subprocess.
 * Adds PYTHONPATH so Python tasks can import from dags/ and sdk/python/.
 */
function buildConduitEnv(): NodeJS.ProcessEnv {
    const fs = require('fs');
    const dagsPath = getDagsPath();
    const extraPaths: string[] = [dagsPath];

    // Look for sdk/python relative to the dags path or workspace
    const workspaceFolders = vscode.workspace.workspaceFolders;
    if (workspaceFolders) {
        for (const folder of workspaceFolders) {
            const root = folder.uri.fsPath;
            const sdkCandidates = [
                path.join(root, 'sdk', 'python'),
                path.join(root, 'conduit', 'sdk', 'python'),
            ];
            for (const sdk of sdkCandidates) {
                if (fs.existsSync(sdk)) {
                    extraPaths.push(sdk);
                }
            }
        }
    }

    const existingPythonPath = process.env.PYTHONPATH || '';
    const pythonPath = [...extraPaths, existingPythonPath].filter(Boolean).join(':');

    return { ...process.env, PYTHONPATH: pythonPath };
}

/**
 * Executes a conduit CLI command and returns the result.
 */
export function runConduitCommand(
    args: string[],
    options?: { cwd?: string; timeout?: number }
): Promise<ConduitCommandResult> {
    return new Promise((resolve) => {
        const binary = getConduitBinary();
        const cwd = options?.cwd || getDagsPath();
        const timeout = options?.timeout || 30000;

        const proc = cp.spawn(binary, args, {
            cwd,
            env: buildConduitEnv(),
            timeout,
        });

        let stdout = '';
        let stderr = '';

        proc.stdout.on('data', (data: Buffer) => {
            stdout += data.toString();
        });

        proc.stderr.on('data', (data: Buffer) => {
            stderr += data.toString();
        });

        proc.on('close', (code: number | null) => {
            resolve({
                stdout,
                stderr,
                exitCode: code ?? 1,
            });
        });

        proc.on('error', (err: Error) => {
            resolve({
                stdout,
                stderr: `Failed to execute conduit: ${err.message}`,
                exitCode: 1,
            });
        });
    });
}

/**
 * Executes a conduit CLI command and streams output to a provided output channel.
 * Returns a promise that resolves with the exit code.
 */
export function runConduitCommandStreaming(
    args: string[],
    outputChannel: vscode.OutputChannel,
    options?: { cwd?: string; timeout?: number }
): Promise<number> {
    return new Promise((resolve) => {
        const binary = getConduitBinary();
        const cwd = options?.cwd || getDagsPath();
        const timeout = options?.timeout || 120000;

        outputChannel.appendLine(`> ${binary} ${args.join(' ')}`);
        outputChannel.appendLine('');

        const proc = cp.spawn(binary, args, {
            cwd,
            env: buildConduitEnv(),
            timeout,
        });

        proc.stdout.on('data', (data: Buffer) => {
            outputChannel.append(data.toString());
        });

        proc.stderr.on('data', (data: Buffer) => {
            outputChannel.append(data.toString());
        });

        proc.on('close', (code: number | null) => {
            outputChannel.appendLine('');
            outputChannel.appendLine(`Process exited with code ${code ?? 1}`);
            resolve(code ?? 1);
        });

        proc.on('error', (err: Error) => {
            outputChannel.appendLine(`Error: ${err.message}`);
            resolve(1);
        });
    });
}

/**
 * Parses DAG information from conduit compile output.
 */
export interface DagInfo {
    id: string;
    taskCount: number;
    schedule: string;
    tasks: TaskInfo[];
}

export interface TaskInfo {
    name: string;
    dependencies: string[];
    isRoot: boolean;
}

export interface CompileResult {
    success: boolean;
    filesScanned: number;
    dagsCompiled: number;
    totalTasks: number;
    errors: number;
    duration: string;
    dags: DagInfo[];
    rawErrors: CompileError[];
}

export interface CompileError {
    file: string;
    line: number;
    column: number;
    message: string;
    severity: 'error' | 'warning';
}

export function parseCompileOutput(stdout: string, stderr: string): CompileResult {
    const result: CompileResult = {
        success: true,
        filesScanned: 0,
        dagsCompiled: 0,
        totalTasks: 0,
        errors: 0,
        duration: '',
        dags: [],
        rawErrors: [],
    };

    const lines = stdout.split('\n');

    // Parse summary section
    for (const line of lines) {
        const filesMatch = line.match(/Files scanned:\s+(\d+)/);
        if (filesMatch) {
            result.filesScanned = parseInt(filesMatch[1], 10);
        }

        const dagsMatch = line.match(/DAGs compiled:\s+(\d+)/);
        if (dagsMatch) {
            result.dagsCompiled = parseInt(dagsMatch[1], 10);
        }

        const tasksMatch = line.match(/Total tasks:\s+(\d+)/);
        if (tasksMatch) {
            result.totalTasks = parseInt(tasksMatch[1], 10);
        }

        const errorsMatch = line.match(/Errors:\s+(\d+)/);
        if (errorsMatch) {
            result.errors = parseInt(errorsMatch[1], 10);
            if (result.errors > 0) {
                result.success = false;
            }
        }

        const durationMatch = line.match(/Duration:\s+(.+)/);
        if (durationMatch) {
            result.duration = durationMatch[1].trim();
        }
    }

    // Parse DAG entries
    let currentDag: DagInfo | null = null;
    for (const line of lines) {
        // Match DAG header: "  dag_name (N tasks) [schedule]"
        const dagMatch = line.match(/^\s{2}(\S+)\s+\((\d+)\s+tasks?\)\s+\[(.+)\]/);
        if (dagMatch) {
            if (currentDag) {
                result.dags.push(currentDag);
            }
            currentDag = {
                id: dagMatch[1],
                taskCount: parseInt(dagMatch[2], 10),
                schedule: dagMatch[3],
                tasks: [],
            };
            continue;
        }

        // Match task entries: "    task_name (root)" or "    task_name <- [dep1, dep2]"
        if (currentDag) {
            const rootTaskMatch = line.match(/^\s{4}(\S+)\s+\(root\)/);
            if (rootTaskMatch) {
                currentDag.tasks.push({
                    name: rootTaskMatch[1],
                    dependencies: [],
                    isRoot: true,
                });
                continue;
            }

            const depTaskMatch = line.match(/^\s{4}(\S+)\s+<-\s+\[(.+)\]/);
            if (depTaskMatch) {
                const deps = depTaskMatch[2].split(',').map((d) => d.trim());
                currentDag.tasks.push({
                    name: depTaskMatch[1],
                    dependencies: deps,
                    isRoot: false,
                });
                continue;
            }
        }
    }

    if (currentDag) {
        result.dags.push(currentDag);
    }

    // Parse errors from stderr or stdout
    const allOutput = stdout + '\n' + stderr;
    const errorRegex = /(?:error|Error)\[?\]?:?\s*(.+?)(?:\s+at\s+|-->)\s*([^:]+):(\d+)(?::(\d+))?/g;
    let errorMatch;
    while ((errorMatch = errorRegex.exec(allOutput)) !== null) {
        result.rawErrors.push({
            message: errorMatch[1].trim(),
            file: errorMatch[2].trim(),
            line: parseInt(errorMatch[3], 10),
            column: errorMatch[4] ? parseInt(errorMatch[4], 10) : 0,
            severity: 'error',
        });
    }

    // Also check for simpler error patterns: "  error: message\n    --> file:line"
    const simpleErrorRegex = /^\s*(error|warning):\s*(.+)\n\s*-->\s*([^:]+):(\d+)(?::(\d+))?/gm;
    while ((errorMatch = simpleErrorRegex.exec(allOutput)) !== null) {
        result.rawErrors.push({
            severity: errorMatch[1] as 'error' | 'warning',
            message: errorMatch[2].trim(),
            file: errorMatch[3].trim(),
            line: parseInt(errorMatch[4], 10),
            column: errorMatch[5] ? parseInt(errorMatch[5], 10) : 0,
        });
    }

    if (result.rawErrors.length > 0) {
        result.success = false;
    }

    return result;
}
