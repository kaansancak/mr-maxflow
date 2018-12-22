from python_graph.digraph import digraph
from python_graph.searching import depth_first_search as dfs
from mrjob.util import to_lines as get_lines
import sys
import json

import ff_mapreduce

def json_to_string(json_obj):
    return json.dumps(json_obj)

def is_terminated(job, current, prev):
    source_counter = job.counters()[0]["move"]["source"]
    new_value = 5 if source_counter != prev else current - 1
    current = new_value
    prev = source_counter
    return current, prev

def augment_graph(graph, augmented_edges):
    # copy graph
    g = digraph()
    g.add_nodes(graph.nodes())
    for edge in graph.edges():
        g.add_edge(edge, wt=graph.edge_weight(edge))

    # update edge weights based on augmentation
    for u in g.nodes():
        for v in g.neighbors(u):
            e_id = "{0},{1}".format(u, v)
            if e_id in augmented_edges:
                flow = augmented_edges[e_id]

                # update forward edge
                f_e = (u, v)
                residue = g.edge_weight(f_e) - flow
                g.set_edge_weight(f_e, residue)
                if residue < 0:
                    print("Fatal error: negative edge residue")
                    sys.exit(-1)

                # update back edge
                r_id = "{0},{1}".format(v, u)
                b_e = (v, u)
                if g.has_edge(b_e):
                    new_weight = g.edge_weight(b_e) + flow
                    g.set_edge_weight(b_e, new_weight)
                else:
                    g.add_edge(b_e, wt=flow)

    # remove edges with zero or less capacity
    for edge in g.edges():
        if g.edge_weight(edge) == 0:
            g.del_edge(edge)

    return g


def run(in_graph_file, is_cloud):    # converts a graph in an acceptable form into a representation

    mr_file_name = "mr_max_flow.txt"
    graph_file = open(in_graph_file, "r")

    original_graph = digraph()
    for line in graph_file:
        kv_pair = line.split("\t")
        vertex = kv_pair[0]
        vertex = json.loads(vertex)
        edges = kv_pair[1]
        edges = json.loads(edges)

        if not original_graph.has_node(vertex):
            original_graph.add_node(vertex)
        for edge in edges:
            v, e_c = edge
            if not original_graph.has_node(v):
                original_graph.add_node(v)
            original_graph.add_edge((vertex, v), wt=e_c)

    s_neighbors = {}
    mr_graph = {}

    for u in original_graph.nodes():
        E_u = []
        S_u = []
        T_u = []

        for v in original_graph.neighbors(u):
            e_c = original_graph.edge_weight((u, v))
            new_edge = [v, "{0},{1}".format(u, v), 0, e_c]
            E_u.append(new_edge)

            # assumes verticies in neighbor list are unique
            if u == "s":
                s_neighbors[v] = new_edge
            if v == "t":
                T_u.append([new_edge])

        mr_graph[u] = [S_u, T_u, E_u]

    for u, edge in s_neighbors.items():
        mr_graph[u][0] = [[edge]]

    #print(mr_graph)
    if "s" not in mr_graph or "t" not in mr_graph:
        print("need to provide source and sink verticies")
        sys.exit(-1)

    outfile = open(mr_file_name, "w")
    for vertex_id, vertex_info in mr_graph.items():
        vertex_info.append({})
        new_line = json.dumps(vertex_id) + "\t" + json.dumps(vertex_info) + "\n"
        outfile.write(new_line)
    outfile.close()

    augmented_edges = {}

    # counters to keep track of convergence of MapReduce jobs
    converge_count = 5
    previous_count = -1

    while converge_count != 0:
        infile = open(mr_file_name, "rb")
        if is_cloud:
            mr_job = ff_mapreduce.MRFlow(args=['-r', 'dataproc'])
        else:
            mr_job = ff_mapreduce.MRFlow()

        # mr_job = max_flow.MRFlow()
        mr_job.stdin = infile
        # print("here")
        with mr_job.make_runner() as runner:
            # perform iteration of MapReduce
            runner.run()

            # process map reduce output
            print(runner.cat_output)
            print("\n")

            out_buffer = []

            for line in get_lines(runner.cat_output()):
                line = line.decode()
                print("DEBUG", line)
                sys.stdout.flush()
                # print str(counter) + ": " + line
                try:
                    kv_pair = line.split("\t")
                    key = kv_pair[0]
                    key = json.loads(key)
                    value = kv_pair[1]
                    value = json.loads(value)
                except:
                    continue

                if key == "A_p":
                    A_p = value
                    for edge, flow in A_p.items():
                        if flow == 0:
                            print("zero flow for edge: " + str(edge))
                        if edge in augmented_edges:
                            augmented_edges[edge] += flow
                        else:
                            augmented_edges[edge] = flow
                else:
                    out_buffer.append(line)

            # write map reduce output to file for next iteration
            outfile = open(mr_file_name, "w")
            for line in out_buffer:
                kv_pair = line.split("\t")
                key = kv_pair[0]
                vertex = json.loads(key)
                value = kv_pair[1]
                value = json.loads(value)

                value.append(A_p)
                new_line = json_to_string(key)
                new_line += "\t"
                new_line += json_to_string(value)
                new_line += "\t"
                outfile.write(new_line)
            
            converge_count, previous_count = is_terminated(runner, converge_count, previous_count)

        infile.close()
        outfile.close()

    augmented_graph = augment_graph(original_graph, augmented_edges)

    # find cut
    preordering = dfs(augmented_graph, "s")[1]
    min_cut = 0
    for edge in original_graph.edges():
        u, v = edge
        if u in preordering and v not in preordering:
            min_cut += original_graph.edge_weight(edge)

    return min_cut

in_graph_file = sys.argv[1]
is_cloud = sys.argv[2] == 'cloud'
max_flow, _ = run(in_graph_file, is_cloud)
print("max_flow={0}".format(max_flow))
sys.stdout.flush()
