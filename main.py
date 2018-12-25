from python_graph.digraph import digraph
from python_graph.searching import depth_first_search as dfs
from mrjob.util import to_lines as get_lines
import sys
import json
import ff_mapreduce

# Check if the Map-Reduce rounds are terminated
def is_terminated(job, current, prev):
    source_counter = job.counters()[0]["move"]["source"]
    # If source counter changes, it is not converged
    new_value = 5 if source_counter != prev else current - 1
    current = new_value
    prev = source_counter
    return current, prev

def json_to_string(json_obj):
    return json.dumps(json_obj)

# Copy a graphs vertices and edges into another empty graph
def copy_graph(other): 
    old_nodes = other.nodes()
    new_graph = digraph()
    new_graph.add_nodes(old_nodes)
    old_edges = other.edges()
    for edge in old_edges:
        edge_weight = other.edge_weight(edge)
        new_graph.add_edge(edge, edge_weight)
    return new_graph

# Read the file, create the graph and start map reduce jobs
def run(in_graph_file, is_cloud):

    # Get the file and read it
    mr_file_name = "mr_max_flow.txt"
    graph_file = open(in_graph_file, "r")
    
    # Generate the original graph thas is read from the file
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


    # Convert the graph into new data structure of key, value pairs
    # nodeID: E_u, S_u, T_u
    neighbors_of_s = {}
    mr_graph = {}

    for source_vertex in original_graph.nodes():
        node_properties = list()
        node_properties.append(list()) # List of edges of vertex
        node_properties.append(list()) # Source excess paths of vertex
        node_properties.append(list()) # Sink excess paths of vertex

        for destionation_vertex in original_graph.neighbors(source_vertex):
            # Create an edge between the node and its neighbors
            edge = (source_vertex, destionation_vertex)
            # Edge = (Destination vertex, edge id, flow, capacity)
            new_edge = [edge[1], edge[0] + "," + edge[1], 0, original_graph.edge_weight(edge)]
            node_properties[0].append(new_edge)

            # Check if the vertices of new edge are source or sink nodes
            is_source = edge[0] == "s"
            is_sink = edge[1] == "t"
            # If so, add an excess path to the list
            if is_source: neighbors_of_s[edge[1]] = new_edge
            if is_sink: node_properties[2].append([new_edge])

        # Add a vertex to the map
        mr_graph[source_vertex] = [node_properties[1], node_properties[2], node_properties[0]]

    # Set the edges of vertex
    for u, edge in neighbors_of_s.items():
        mr_graph[u][0] = [[edge]]


    # Open a file and write the newly generated structure
    outfile = open(mr_file_name, "w")
    for key in mr_graph:
        mr_graph[key].append({})
        new_line = json_to_string(key)
        new_line += "\t"
        new_line += json_to_string(mr_graph[key])
        new_line += "\n"
        outfile.write(new_line)
    outfile.close()

    # Create a dictionary to keep the augmented edges
    augmented_edges = {}

    # Create counters to keep track of convergence
    converge_count = 1
    previous_count = -1

    # Continue to perform Map-Reduce rounds until the max-flow value converges
    while converge_count != 0:
        # Read the input from file that contains in the new data structure form
        infile = open(mr_file_name, "rb")

        # Set the environment
        if is_cloud:
            mr_job = ff_mapreduce.MRFlow(args=['-r', 'dataproc'])
        else:
            mr_job = ff_mapreduce.MRFlow()

        # Give the job to the MRJob library and make it run the round
        mr_job.stdin = infile
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

        # Write the output of Map-Reduce round so that the next round can start by reading it
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

        # Check the convergence values after the round
        converge_count, previous_count = is_terminated(runner, converge_count, previous_count)

        # Close input and output files
        infile.close()
        outfile.close()

    # Copy the graph into another in order to augment its edges
    augmented_graph = copy_graph(original_graph)
    nodes = augmented_graph.nodes()

    # Update the each node by using the augmentation results
    for node in nodes:
        for neighbor in augmented_graph.neighbors(node):
            # Chosen a node, neigbor pair
            vertex_pair = [node, neighbor]
            
            # Get the edge and check if it is augmented
            edge = vertex_pair[0] + "," + vertex_pair[1]
            is_edge_augmented = edge in augmented_edges
        
            # Set the flow if it is augmented
            if is_edge_augmented:
                flow = augmented_edges[edge]

                # Set the forward edges new weight by substracting the flow
                residue = augmented_graph.edge_weight((vertex_pair[0], vertex_pair[1])) - flow
                augmented_graph.set_edge_weight((vertex_pair[0], vertex_pair[1]), residue)
                if residue < 0:
                    sys.exit(-1)

                # Set the back edges new weight by adding the flow, create if it doesn't exist
                if augmented_graph.has_edge((vertex_pair[1], vertex_pair[0])):
                    new_weight = augmented_graph.edge_weight((vertex_pair[1], vertex_pair[0])) + flow
                    augmented_graph.set_edge_weight((vertex_pair[1], vertex_pair[0]), new_weight)
                else:
                    augmented_graph.add_edge((vertex_pair[1], vertex_pair[0]), wt=flow)

    # Find the zero edges and remove them from the augmented graph
    zero_edges = list(filter(lambda edge: augmented_graph.edge_weight(edge) == 0, augmented_graph.edges()))
    for edge in augmented_graph.edges():
        if edge in zero_edges: augmented_graph.del_edge(edge)

    # Perform a depth first search starting from source node and get its preordering
    cut_nodes = dfs(augmented_graph, "s")[1]
    edges = original_graph.edges()

    # Find the edges that are connections between source part and sink part of the cut
    edges = list(filter(lambda edge: edge[0] in cut_nodes and edge[1] not in cut_nodes, edges))

    # Get the edge weights which are flow values
    edge_weights = list(map(lambda edge: original_graph.edge_weight(edge), edges))
    max_flow = sum(edge_weights)

    return max_flow

# Get the graph file
in_graph_file = sys.argv[1]

# Check if the running environment is set
if len(sys.argv) != 3:
    print("Please choose input file and environment to be run")
    print("Example usage: python3 main.py graph_file.txt local")
    print("Example usage: python3 main.py graph_file.txt cloud")
    sys.exit()

# Get and set the environment
if sys.argv[2] == "cloud":
    is_cloud = True
elif sys.argv[2] == "local":
    is_cloud = False
else:
    print("Environment must be 'local' or 'cloud'")
    sys.exit()

# Run the job and get the max flow
max_flow = run(in_graph_file, is_cloud)
print("max_flow:", max_flow)