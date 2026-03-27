from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from collections import defaultdict, deque

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class PipelineData(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}


# 🔥 Improved DAG check
def is_dag(nodes, edges):
    node_ids = {node['id'] for node in nodes}

    # Build adjacency list safely
    adj = {node_id: [] for node_id in node_ids}

    for edge in edges:
        src = edge.get('source')
        tgt = edge.get('target')

        if src in node_ids and tgt in node_ids:
            adj[src].append(tgt)

    visited = set()
    rec_stack = set()

    def dfs(node):
        if node in rec_stack:
            return False  # cycle detected

        if node in visited:
            return True

        visited.add(node)
        rec_stack.add(node)

        for neighbor in adj[node]:
            if not dfs(neighbor):
                return False

        rec_stack.remove(node)
        return True

    # Check all components
    for node in node_ids:
        if node not in visited:
            if not dfs(node):
                return False

    return True


@app.post('/pipelines/parse')
async def parse_pipeline(data: PipelineData):
    nodes = data.nodes
    edges = data.edges

    # ✅ Add validation here
    if not nodes:
        return {
            "num_nodes": 0,
            "num_edges": 0,
            "is_dag": False,
            "error": "No nodes provided"
        }

    num_nodes = len(nodes)
    num_edges = len(edges)
    is_dag_result = is_dag(nodes, edges)

    return {
        "num_nodes": num_nodes,
        "num_edges": num_edges,
        "is_dag": is_dag_result
    }
    

    
def get_execution_order(nodes, edges):
    node_ids = [node['id'] for node in nodes]

    adj = defaultdict(list)
    indegree = {node_id: 0 for node_id in node_ids}

    for edge in edges:
        src = edge['source']
        tgt = edge['target']
        if tgt in indegree:
            adj[src].append(tgt)
            indegree[tgt] += 1

    queue = deque([n for n in node_ids if indegree[n] == 0])
    order = []

    while queue:
        node = queue.popleft()
        order.append(node)

        for neighbor in adj[node]:
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                queue.append(neighbor)

    return order


def execute_node(node, inputs):
    node_type = node.get('type')
    data = node.get('data', {})

    if node_type == 'customInput':
        return data.get('value', '')

    elif node_type == 'text':
        text = data.get('text', '')
        # Direct variable replacement for all connected handles
        for handle_id, val in inputs.items():
            text = text.replace(f'{{{{{handle_id}}}}}', str(val))
        return text

    elif node_type == 'llm':
        # LLM usually has 'system' and 'prompt' handles
        system = inputs.get('system', '')
        prompt = inputs.get('prompt', '')
        combined = f"{system} {prompt}".strip()
        if not combined:
            combined = " ".join(map(str, inputs.values()))
        return f"AI says: {combined}"

    elif node_type == 'customOutput':
        return " ".join(map(str, inputs.values()))

    elif node_type == 'logic':
        op = data.get('op', 'AND')
        vals = [bool(v) for v in inputs.values()]
        if op == 'AND':
            return all(vals) if vals else False
        elif op == 'OR':
            return any(vals) if vals else False
        elif op == 'NOT':
            # Use 'a' handle if available, otherwise first input
            val = inputs.get('a', vals[0] if vals else True)
            return not bool(val)
        return False

    elif node_type == 'math':
        op = data.get('op', 'ADD')
        try:
            # Try to use 'a' and 'b' handles specifically, fallback to all values
            a = float(inputs.get('a', 0))
            b = float(inputs.get('b', 0))
            vals = [float(v) for v in inputs.values() if v is not None and str(v).strip() != '']
            
            if op == 'ADD': return sum(vals) if vals else a + b
            elif op == 'SUB': return a - b
            elif op == 'MUL':
                if 'a' in inputs and 'b' in inputs: return a * b
                res = 1
                for v in vals: res *= v
                return res
            elif op == 'DIV':
                return a / b if b != 0 else "Error: Div by 0"
        except:
            return "Error: Invalid Input"

    elif node_type == 'note':
        return data.get('note', '')

    elif node_type == 'alert':
        return data.get('message', 'Alert!')

    elif node_type == 'timer':
        return data.get('duration', 1000)

    return " ".join(map(str, inputs.values()))


@app.post('/pipelines/run')
async def run_pipeline(data: PipelineData):
    nodes = data.nodes
    edges = data.edges

    node_map = {node['id']: node for node in nodes}
    execution_order = get_execution_order(nodes, edges)

    results = {}

    for node_id in execution_order:
        node = node_map[node_id]

        # Map targetHandle to the value from the source node
        node_inputs = {}
        for edge in edges:
            if edge['target'] == node_id:
                handle_id = edge.get('targetHandle', 'default')
                src_id = edge['source']
                if src_id in results:
                    node_inputs[handle_id] = results[src_id]

        try:
            result = execute_node(node, node_inputs)
            results[node_id] = result
        except Exception as e:
            result = f"Error: {str(e)}"
            results[node_id] = result

    return {
        "results": results
    }