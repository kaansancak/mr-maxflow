from python_graph.digraph import digraph
from python_graph.searching import depth_first_search as dfs
from mrjob.util import to_lines as get_lines
import sys
import json

import ff_mapreduce

def is_terminated(job, current, prev):
    source_counter = job.counters()[0]["move"]["source"]
    new_value = 5 if source_counter != prev else current - 1
    current = new_value
    prev = source_counter
    return current, prev


def json_to_string(json_obj):
    return json.dumps(json_obj)

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


def run(in_graph_file, is_cloud):
    # converts a graph in an acceptable form into a representation
    # that is usable in MapReduce
    mr_file_name = "mr_max_flow.txt"
   
    graph_file = open(in_graph_file, "r")
    original_graph = digraph()
    for line in graph_file:
        kv_pair = line.split("\t")
        vertex = kv_pair[0]
        vertex = json.loads(vertex)
        edges = kv_pair[1]
        edges = json.loads(edges)

        node_exists = original_graph.has_node(vertex)
        if node_exists == False: original_graph.add_node(vertex)

        for edge in edges:
            node_exists = original_graph.has_node(edge[0])
            if node_exists == False: original_graph.add_node(edge[0])
            original_graph.add_edge((vertex, edge[0]), wt=edge[1])

    neighbors_of_s = {}
    mr_graph = {}

    for source_vertex in original_graph.nodes():
        node_properties = list()
        node_properties.append(list())
        node_properties.append(list())
        node_properties.append(list())

        for destionation_vertex in original_graph.neighbors(source_vertex):
            edge = (source_vertex, destionation_vertex)
            new_edge = [edge[1], edge[0] + "," + edge[1], 0, original_graph.edge_weight(edge)]
            node_properties[0].append(new_edge)

            is_source = edge[0] == "s"
            is_sink = edge[1] == "t"
            if is_source: neighbors_of_s[edge[1]] = new_edge
            if is_sink: node_properties[2].append([new_edge])

        mr_graph[source_vertex] = [node_properties[1], node_properties[2], node_properties[0]]

    for u, edge in neighbors_of_s.items():
        mr_graph[u][0] = [[edge]]

    outfile = open(mr_file_name, "w")
    for key in mr_graph:
        mr_graph[key].append({})
        new_line = json_to_string(key)
        new_line += "\t"
        new_line += json_to_string(mr_graph[key])
        new_line += "\n"
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
        runner = mr_job.make_runner()
        runner.run()
        out_buffer = []

        for line in get_lines(runner.cat_output()):
            line = line.decode()
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
                for edge in A_p:
                    is_edge_augmented = edge in augmented_edges
                    augmented_edges[edge] = augmented_edges[edge] + A_p[edge] if is_edge_augmented else A_p[edge]
            else:
                out_buffer.append(line)

        # write map reduce output to file for next iteration
        outfile = open(mr_file_name, "w")
        for line in out_buffer:
            kv_pair = line.split("\t")
            key = kv_pair[0]
            key = json.loads(key)
            value = kv_pair[1]
            value = json.loads(value)

            value.append(A_p)
            new_line = json_to_string(key)
            new_line += "\t"
            new_line += json_to_string(value)
            new_line += "\n"
            outfile.write(new_line)

        converge_count, previous_count = is_terminated(runner, converge_count, previous_count)

        infile.close()
        outfile.close()

    augmented_graph = augment_graph(original_graph, augmented_edges)

    cut_nodes = dfs(augmented_graph, "s")[1]
    edges = original_graph.edges()
    edges = list(filter(lambda edge: edge[0] in cut_nodes and edge[1] not in cut_nodes, edges))
    edge_weights = list(map(lambda edge: original_graph.edge_weight(edge), edges))
    max_flow = sum(edge_weights)

    return max_flow

in_graph_file = sys.argv[1]
is_cloud = False
max_flow = run(in_graph_file, is_cloud)
print("max_flow:", max_flow)