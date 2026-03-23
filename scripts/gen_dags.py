#!/usr/bin/env python3
"""Generate benchmark DAGs quickly. Usage: python3 gen_dags.py <dir> <count> [--yaml]"""
import sys, os

def gen_python(dag_id, tasks=4):
    lines = [f'from conduit_sdk import dag, task\n\n@dag(schedule="0 6 * * *", tags=["bench"])\ndef {dag_id}():\n    """Bench DAG."""\n']
    for i in range(tasks):
        dep = f"out_{i-1}" if i > 0 else ""
        param = dep if dep else ""
        lines.append(f"    @task()\n    def task_{i}({param}):\n        pass\n")
    lines.append("    # wiring")
    for i in range(tasks):
        if i == 0:
            lines.append(f"    out_0 = task_0()")
        else:
            lines.append(f"    out_{i} = task_{i}(out_{i-1})")
    return "\n".join(lines) + "\n"

def gen_yaml(dag_id, tasks=4):
    lines = [f"id: {dag_id}", f'schedule: "0 6 * * *"', "tasks:"]
    for i in range(tasks):
        lines.append(f"  task_{i}:")
        lines.append(f"    type: shell")
        lines.append(f"    command: 'echo task_{i}'")
        if i > 0:
            lines.append(f"    depends_on: [task_{i-1}]")
    return "\n".join(lines) + "\n"

if __name__ == "__main__":
    target = sys.argv[1]
    count = int(sys.argv[2])
    use_yaml = "--yaml" in sys.argv
    os.makedirs(target, exist_ok=True)
    for i in range(count):
        name = f"dag_{i:04d}"
        if use_yaml:
            with open(os.path.join(target, f"{name}.yaml"), "w") as f:
                f.write(gen_yaml(name))
        else:
            with open(os.path.join(target, f"{name}.py"), "w") as f:
                f.write(gen_python(name))
