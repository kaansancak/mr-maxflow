import json
import sys
import random
import networkx as nx

if __name__ == '__main__':
    args = sys.argv

    num_nodes = int(sys.argv[1])
    ws = nx.watts_strogatz_graph(num_nodes, 100,0.20)
    wnodes = list(nx.nodes(ws))
    outfile_path = sys.argv[2]
    nodes = ["s", "t"]
    for i in range(len(wnodes)):
        nodes.append(str(wnodes[i]))
    sink_connected = 0
    t_connected = 0
    sink_connect = 0
    graph = {}

    for u in nodes:
        graph[u] = []

    edges = list(nx.edges(ws))
    for v in (edges):
        if str(v[1]) != str(v[0]) and str(v[1]) != "t":  
            random_weight = random.randint(1, 10)
            graph[str(v[1])].append([str(v[0]), random_weight])
    
    for i in nodes:
        if str(i) != "t":  
            random_weight = random.randint(1, 10)
            prob_edge = random.random()
            if prob_edge > 0.9 and sink_connect < 1500:
                sink_connect += 1
                graph["s"].append([str(i), random_weight])
            
            prob_edge = random.random()
            
            if str(i) != "s": 
                if prob_edge > 0.7:
                    prob_edge = random.random()
                    if prob_edge > 0.5 and sink_connected < 1500:
                        graph[str(i)].append([str("s"), random_weight])
                    elif t_connected < 1500:
                        graph[str(i)].append([str("t"), random_weight])



    outfile = open(outfile_path, "w")
    for key in graph:
        outfile.write(json.dumps(key) + "\t" + json.dumps(graph[key]) + "\n")

    outfile.close()
