import mrjob.job, mrjob.protocol, mrjob.step, sys

class Accumulator:
    def __init__(self):
        self.edges = dict()

    def accept(self, augmenting_path):
        # assume path is valid, set min_flow to a big number
        valid_path, min_flow = (True, 1000000)

        # edge[0] = destination vertex
        # edge[1] = edge_id (vertex1_id, vertex2_id)
        # edge[2] = flow on edge
        # edge[3] = capacity of edge

        for edge in augmenting_path:
            # attempt to get edge flow by id
            edge_option = self.edges.get(edge[1], None)

            if edge_option == None:
                # if edge isn't already in path, init flow to 0
                accumulated_flow = 0
            else:
                # else init flow to existing flow
                accumulated_flow = edge_option

            # set residue to (capacity) - (accumulated_flow) - (flow on edge)
            residue = edge[3]
            residue -= accumulated_flow
            residue -= edge[2]

            # if residue is non positive, augmenting path is invalid
            valid_path = False if residue <= 0 else valid_path
            # if residue is positive and smaller than min_flow on path, set min_flow to residue
            min_flow = residue if (residue > 0 and residue < min_flow) else min_flow

        if (valid_path == False):
            # reject path if invalid
            return False
        else:
            for edge in augmenting_path:
                # check if edge is in path
                id_option = self.edges.get(edge[1], None)
                # if it is not, set flow on edge to min_flow
                # else set flow on edge to (flow on edge + min_flow)
                self.edges[edge[1]] = self.edges[edge[1]] + min_flow if id_option != None else min_flow
            return True

# accumulators don't accept more source excess paths than this
MAX_PATHS = 10

### helpers for map function ###

# check cycle before adding a new edge to path
def edge_forms_cycle(new_edge, path):
    return any(map(lambda edge: edge[0] == new_edge[0], path))

# update flow of edge in augmented_edges
# add edge to saturated_edges if flow >= capacity
def update_edge(edge, augmented_edges, saturated_edges):
    edge_option = augmented_edges.get(edge[1], None)

    if edge_option != None:
        edge[2] = edge[2] + augmented_edges[edge[1]]

    if edge_option!= None and edge[2] >= edge[3] and saturated_edges.count(edge[1]) == 0:
        saturated_edges.append(edge[1])

### MRJob job definition ###
class MRFlow(mrjob.job.MRJob):
    OUTPUT_PROTOCOL, INPUT_PROTOCOL = (mrjob.protocol.JSONProtocol, mrjob.protocol.JSONProtocol)

    def mapper(self, u, node_info):
        saturated_edges = list()

        # intermediate result from previous job
        # contains edges that were augmented before
        augmented_edges = node_info[-1]
        del node_info[-1]

        # node_info[0]: S_u source excess paths
        # node_info[1]: T_u sink excess paths
        # node_info[2]: E_u list of edges connecting u to neighbors

        # update source and sink excess paths and edges from u according to augmented edges
        # and add saturated edges
        for edge in node_info[2]:
            update_edge(edge, augmented_edges, saturated_edges)
        for path in node_info[0]:
            for edge in path:
                update_edge(edge, augmented_edges, saturated_edges)
        for path in node_info[1]:
            for edge in path:
                update_edge(edge, augmented_edges, saturated_edges)

        # remove source and sink excess paths that are saturated
        for e_id in saturated_edges:
            for path in node_info[0]:
                if path.count(e_id) > 0:
                    node_info[0].remove(path)

            for path in node_info[1]:
                if path.count(e_id) > 0:
                    node_info[1].remove(path)

        # try to connect source excess paths to sink excess paths
        accumulator = Accumulator()

        for source_path in node_info[0]:
            for sink_path in node_info[1]:
                if accumulator.accept(source_path + sink_path):
                    yield ("t", [[source_path + sink_path], [], []])

        # regenerate neighbours of s to search new source excess paths
        if u == "s":
            for edge in node_info[2]:
                yield (edge[0], [[[[edge[0], edge[1], edge[2], edge[3]]]], list(), list()])

        # extend source excess paths
        if len(node_info[0]) > 0:
            for edge in node_info[2]:
                e_v, e_f, e_c = edge[0], edge[2], edge[3]
                if edge[2] < edge[3]:
                    for source_path in node_info[0]:
                        if not edge_forms_cycle(edge, source_path):
                            yield (e_v, [[source_path + [edge]], list(), list()])

        yield (u, [node_info[0], node_info[1], node_info[2]])

    def reducer(self, u, values):
        # initialize new Accumulators
        A_p = Accumulator()
        A_s = Accumulator()
        A_t = Accumulator()

        S_u = list() # source excess paths
        T_u = list() # sink excess paths
        E_u = list() # list of edges connecting u to neighbours

        # val[0] = source excess paths to vertex
        # val[1] = sink excess paths to vertex
        # val[2] = list of edges connecting vertex to neighbour
        for val in values:
            if len(val[2]) > 0:
                E_u = val[2]

            # merge and filter S_v
            for se in val[0]:
                if u == "t":
                    A_p.accept(se)
                if u != "t" and len(S_u) < MAX_PATHS and A_s.accept(se):
                    S_u.append(se)

            for te in val[1]:
                if len(T_u) < MAX_PATHS and A_t.accept(te):
                    T_u.append(te)

        # initalize counter
        counter_init = [("move", "source", 0), ("move", "source", 0), ("move", "source", len(S_u))]
        for item in counter_init:
            self.increment_counter(item[0], item[1], item[2])

        is_sink = u== "t"
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
