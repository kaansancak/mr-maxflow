import mrjob.job, mrjob.protocol, mrjob.step, sys

# Accumulator is the class that we used for deciding whether to accept
# augmenting paths in our map-reduce jobs.
# As long as the edges given to the accumulator does not violate
# capacity contraints of edges that are already exists in the accumulator, 
# the acculumator accepts the paths.
class Accumulator:
    def __init__(self):
        self.edges = dict()

    def accept(self, augmenting_path):
        valid_path, min_flow = (True, 1000000)

        # Check whether the edge violates the capacity contrainsts
        # Calculate the flow amount that can be puushed through the path
        for edge in augmenting_path:
            edge_option = self.edges.get(edge[1], None)

            if edge_option == None:
                accumulated_flow = 0
            else:
                accumulated_flow = edge_option

            residue = edge[3]
            residue -= accumulated_flow
            residue -= edge[2]

            valid_path = False if residue <= 0 else valid_path
            min_flow = residue if (residue > 0 and residue < min_flow) else min_flow

        # Update the edges in the accumulator
        if (valid_path == False):
            return False
        else:
            for edge in augmenting_path:
                e_v = edge[0]
                e_id = edge[1]
                e_f = edge[2]
                e_c = edge[3]
                r = edge[1].split(",")
                e_r = str(r[1]) + "," + str(r[0])

                id_option = self.edges.get(edge[1], None)
                self.edges[edge[1]] = self.edges[edge[1]] + min_flow if id_option != None else min_flow
            return True

# We define the maximum number of source exess paths
# that can be accepted by the accumulator as 10
MAX_PATHS = 10


# Check whether the edges forms a cycle
# and make sure that adding a new edge does not creates a cycle
def edge_forms_cycle(new_edge, path):
    return any(map(lambda edge: edge[0] == new_edge[0], path))

# Update the flow of the augmented edges
# Check for the capacity constraint
# if the flow value is equal or greater than the
# capacity add the edge to the satured edges
def update_edge(edge, augmented_edges, saturated_edges):
    edge_option = augmented_edges.get(edge[1], None)

    if edge_option != None:
        edge[2] = edge[2] + augmented_edges[edge[1]]

    if edge_option!= None and edge[2] >= edge[3] and saturated_edges.count(edge[1]) == 0:
        saturated_edges.append(edge[1])


class MRFlow(mrjob.job.MRJob):
    # We decided to use JSON formatting in our
    # input-output formats
    OUTPUT_PROTOCOL, INPUT_PROTOCOL = (mrjob.protocol.JSONProtocol, mrjob.protocol.JSONProtocol)

    def mapper(self, u, node_info):
        saturated_edges = list()

        # augmented_edges contains edges that are
        # augmented from previous round of the map-reduce job
        augmented_edges = node_info[-1]
        del node_info[-1]

        # update edges in sink excess, source excess and edges
        # with augmented edges
        # note that update_edge addes saturated edges to satured_edges
        for edge in node_info[2]:
            update_edge(edge, augmented_edges, saturated_edges)
        for path in node_info[0]:
            for edge in path:
                update_edge(edge, augmented_edges, saturated_edges)
        for path in node_info[1]:
            for edge in path:
                update_edge(edge, augmented_edges, saturated_edges)

        # remove satured excess paths from source excess and sink excess
        for e_id in saturated_edges:
            for path in node_info[0]:
                if path.count(e_id) > 0:
                    node_info[0].remove(path)

            for path in node_info[1]:
                if path.count(e_id) > 0:
                    node_info[1].remove(path)

        # create a accumulator to decide whether the accept new augmenting edeges
        accumulator = Accumulator()

        # combine sink and source excess paths if possible
        for source_path in node_info[0]:
            for sink_path in node_info[1]:
                if accumulator.accept(source_path + sink_path):
                    yield ("t", [[source_path + sink_path], [], []])

        # Seed sink vertex to neigbouring vertices
        if u == "s":
            for edge in node_info[2]:
                yield (edge[0], [[[[edge[0], edge[1], edge[2], edge[3]]]], list(), list()])

        # extend the source excess paths
        if len(node_info[0]) > 0:
            for edge in node_info[2]:
                e_v, e_f, e_c = edge[0], edge[2], edge[3]
                if edge[2] < edge[3]:
                    for source_path in node_info[0]:
                        if not edge_forms_cycle(edge, source_path):
                            yield (e_v, [[source_path + [edge]], list(), list()])

        # emit the intermediate values
        yield (u, [node_info[0], node_info[1], node_info[2]])

    def reducer(self, u, values):

        # initialize acculumulators in order to decide whether to accept paths
        A_p = Accumulator()
        A_s = Accumulator()
        A_t = Accumulator()

        S_m = list()
        T_m = list()
        S_u = list()
        T_u = list()
        E_u = list()

        for val in values:

            # Get the sink excess, source excess and edges values
            # for master vertex with key u
            if len(val[2]) > 0:
                S_m = val[0]
                T_m = val[1]
                E_u = val[2]

            # merge and filter the paths
            for se in val[0]:
                if u == "t":
                    A_p.accept(se)
                if u != "t" and len(S_u) < MAX_PATHS and A_s.accept(se):
                    S_u.append(se)

            for te in val[1]:
                if len(T_u) < MAX_PATHS and A_t.accept(te):
                    T_u.append(te)

        # set the counter values
        counter_init = [("move", "source", 0), ("move", "source", 0), ("move", "source", len(S_u))]
        
        # increment the counters of the current running job
        for item in counter_init:
            self.increment_counter(item[0], item[1], item[2])

        # emit results
        is_sink = u== "t"

        # if the master vertex is the sink vertex
        # also append the resulting values to A_p
        if is_sink:
            key = "A_p"
            value = A_p.edges
            yield key, value

        key = u
        value = list()
        value.append(S_u)
        value.append(T_u)
        value.append(E_u)

        yield key, value

    def steps(self):
        return [mrjob.step.MRStep(mapper=self.mapper, reducer=self.reducer)]

    def __init__(self, *args, **kwargs):
        super(MRFlow, self).__init__(*args, **kwargs)

if __name__ == '__main__':
    MRFlow.run()
