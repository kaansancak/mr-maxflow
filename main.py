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

def copy_graph(other): 
    old_nodes = other.nodes()
    new_graph = digraph()
    new_graph.add_nodes(old_nodes)
    old_edges = other.edges()
    for edge in old_edges:
        edge_weight = other.edge_weight(edge)
        new_graph.add_edge(edge, edge_weight)
    return new_graph

def augment_graph(graph, augmented_edges):

    new_graph = copy_graph(graph)
    nodes = new_graph.nodes()

    for node in nodes:
        for neighbor in new_graph.neighbors(node):
            vertex_pair = [node, neighbor]

            edge = vertex_pair[0] + "," + vertex_pair[1]
            is_edge_augmented = edge in augmented_edges
            if is_edge_augmented:
                flow = augmented_edges[edge]

                residue = new_graph.edge_weight((vertex_pair[0], vertex_pair[1])) - flow
                new_graph.set_edge_weight((vertex_pair[0], vertex_pair[1]), residue)
                if residue < 0:
                    sys.exit(-1)

                if new_graph.has_edge((vertex_pair[1], vertex_pair[0])):
                    new_weight = new_graph.edge_weight((vertex_pair[1], vertex_pair[0])) + flow
                    new_graph.set_edge_weight((vertex_pair[1], vertex_pair[0]), new_weight)
                else:
                    new_graph.add_edge((vertex_pair[1], vertex_pair[0]), wt=flow)

    zero_edges = list(filter(lambda edge: new_graph.edge_weight(edge) == 0, new_graph.edges()))
    for edge in new_graph.edges():
        if edge in zero_edges: new_graph.del_edge(edge)

    return new_graph


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

    augmented_graph = copy_graph(original_graph)
    nodes = augmented_graph.nodes()

    for node in nodes:
        for neighbor in augmented_graph.neighbors(node):
            vertex_pair = [node, neighbor]

            edge = vertex_pair[0] + "," + vertex_pair[1]
            is_edge_augmented = edge in augmented_edges
            if is_edge_augmented:
                flow = augmented_edges[edge]

                residue = augmented_graph.edge_weight((vertex_pair[0], vertex_pair[1])) - flow
                augmented_graph.set_edge_weight((vertex_pair[0], vertex_pair[1]), residue)
                if residue < 0:
                    sys.exit(-1)

                if augmented_graph.has_edge((vertex_pair[1], vertex_pair[0])):
                    new_weight = augmented_graph.edge_weight((vertex_pair[1], vertex_pair[0])) + flow
                    augmented_graph.set_edge_weight((vertex_pair[1], vertex_pair[0]), new_weight)
                else:
                    augmented_graph.add_edge((vertex_pair[1], vertex_pair[0]), wt=flow)

    zero_edges = list(filter(lambda edge: augmented_graph.edge_weight(edge) == 0, augmented_graph.edges()))
    for edge in augmented_graph.edges():
        if edge in zero_edges: augmented_graph.del_edge(edge)

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