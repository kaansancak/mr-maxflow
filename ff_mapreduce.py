import mrjob.job, mrjob.protocol, mrjob.step, sys

class Accumulator:
    def __init__(self):
        self.edges = dict()

    def accept(self, augmenting_path):
        valid_path, min_flow = (True, 1000000)

        for index, edge in enumerate(augmenting_path):
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

MAX_PATHS = 10

def edge_forms_cycle(new_edge, path):
    return any(path.map(lambda edge: edge[0] == new_edge[0]))

def update_edge(edge, augmented_edges, saturated_edges):
    edge_option = augmented_edges.get(edge[1], None)

    if edge_option != None:
        edge[2] = edge[2] + augmented_edges[edge[1]]

    if edge_option!= None and edge[2] >= edge[3] and saturated_edges.count(edge[1]) == 0:
        saturated_edges.append(edge[1])

class MRFlow(mrjob.job.MRJob):
    OUTPUT_PROTOCOL, INPUT_PROTOCOL = (mrjob.protocol.JSONProtocol, mrjob.protocol.JSONProtocol)

    def mapper(self, u, node_info):
        saturated_edges = list()
        augmented_edges = node_info[-1]
        del node_info[-1]

        for edge in node_info[2]:
            update_edge(edge, augmented_edges, saturated_edges)
        for path in node_info[0]:
            for edge in path:
                update_edge(edge, augmented_edges, saturated_edges)
        for path in node_info[1]:
            for edge in path:
                update_edge(edge, augmented_edges, saturated_edges)

        # remove saturated excess paths
        for e_id in saturated_edges:
            for path in node_info[0]:
                if path.count(e_id) > 0:
                    node_info[0].remove(path)

            for path in node_info[1]:
                if path.count(e_id) > 0:
                    node_info[1].remove(path)

        # attempt to combine source and sink excess paths
        accumulator = Accumulator()

        for source_path in node_info[0]:
            for sink_path in node_info[1]:
                if accumulator.accept(source_path + sink_path):
                    yield ("t", [[source_path + sink_path], [], []])

        if u == "s":
            for edge in node_info[2]:
                yield (edge[0], [[[[edge[0], edge[1], edge[2], edge[3]]]], list(), list()])

        # extends source excess paths
        if len(node_info[0]) > 0:
            for edge in node_info[2]:
                e_v, e_f, e_c = edge[0], edge[2], edge[3]
                if edge[2] < edge[3]:
                    for source_path in node_info[0]:
                        if not edge_forms_cycle(edge, source_path):
                            yield (e_v, [[source_path + [edge]], list(), list()])

        yield (u, [node_info[0], node_info[1], node_info[2]])

    def reducer(self, u, values):
        A_p = Accumulator()
        A_s = Accumulator()
        A_t = Accumulator()

        S_m = list()
        T_m = list()
        S_u = list()
        T_u = list()
        E_u = list()

        for val in values:
            if len(values[2]) > 0:
                S_m = val[0]
                T_m = val[1]
                E_u = val[2]

            # merge and filter S_v
            for se in val[0]:
                if u == "t":
                    A_p.accept(se)
                if u != "t" and len(S_u) < MAX_PATHS and A_s.accept(se):
                    S_u.append(se)

            for te in T_v:
                if len(T_u) < MAX_PATHS and A_t.accept(te):
                    T_u.append(te)

        counter_init = [("move", "source", 0), ("move", "source", 0), ("move", "source", len(S_u))]
        for item in counter_init:
            self.increment_counter(counter_init[0], counter_init[1], counter_init[2])

        if u == "t":
            yield "A_p", A_p.edges

        yield (u, [S_u, T_u, E_u])

    def steps(self):
        return [mrjob.step.MRStep(mapper=self.mapper, reducer=self.reducer)]

    def __init__(self, *args, **kwargs):
        super(MRFlow, self).__init__(*args, **kwargs)

if __name__ == '__main__':
    MRFlow.run()
