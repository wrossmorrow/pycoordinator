
from __future__ import annotations

from typing import Any, List, Dict, Optional, Union, Tuple

class Graph:

    """ (Directed Acyclic) Graphs
    directed graph using DFS (Tarjan's Alg) O(V+E)

    This class represents a directed graph using a dict, 
    with the dict keys as the node list and arrays of their
    adjacent nodes (outgoing edges). Nodes can be any hashable
    type. 

    Methods for: 

    * getting the node list
    * getting the edge list
    * getting "roots" (no incoming edges)
    * getting "leaves" (no outgoing edges)
    * "reversing" the graph, by flipping edge orientation
    * adding nodes/edges
    * removing nodes/edges
    * computing the strongly connected components
    * determining if the graph (DAG) is cyclical (has any cycles)
    * finding node-node paths

    """

    def __init__(
        self, 
        nodes: Optional[List[Any]]=[], 
        edges: Optional[List[Tuple[Any]]]=[],
    ) -> Graph:
        self.G = {} # dictionary to store DAG
        self.cc = None

        for u, v in edges:
            self.add(u, v)
        for v in nodes:
            self.add(v)

    def __repr__(self) -> str:
        """ return the graph as a string """
        return f"{self.G}"

    def __contains__(self, v) -> bool:
        """ return True if v is a node """
        return v in self.G

    def is_edge(self, u: Any, v: Any) -> bool:
        if u is None or v is None: 
            return False
        if u not in self.G or v not in self.G:
            return False
        return (v in self.G[u])

    def nodes(self) -> set:
        """ get a set of all nodes """
        return set(list(self.G.keys()))

    def edges(self) -> set:
        """ get a list of all edges, as node tuples """
        ed = []
        for u in self.G: 
            for v in self.G[u]:
                ed.append((u,v))
        return set(ed)
        
    def roots(self) -> set:
        """ roots: those nodes that do not have any incoming edges """
        rts = list(self.G.keys())
        for u in self.G:
            for v in self.G[u]:
                if v in rts: 
                    rts.remove(v)
        return set(rts)

    def leaves(self) -> set:
        """ leaves: those nodes that do not have any outgoing edges """
        return set([v for v in self.G if len(self.G[v]) == 0])

    def intermediates(self) -> set:
        """ intermediates: not roots and not leaves 
        In other words, all nodes v such that 
        (a) some edge ends at v
        (b) some edge starts at v
        """
        s = set({})
        for v in self.G:
            if len(self.G[v]) > 0: 
                s = s | {v} # some edge starts at v
                for u in self.G[v]:
                    s = s | {u} # some edge ends at u
        return s

    def reverse(self) -> Graph:
        G = Graph()
        for u, v in self.edges():
            G.add(v, u)
        return G

    def add(self, u: Any, v: Optional[Any]=None) -> Graph:
        """ add a node or an edge; chainable """

        if u is None:
            return self

        # invalidate SCCs
        self.cc = None

        if u not in self.G:
            self.G[u] = []

        if v is not None: 
            if v not in self.G:
                self.G[v] = []
            self.G[u].append(v)

        return self

    def remove(self, u: Any, v: Optional[Any]=None) -> Graph:
        """ remove a node or an edge; chainable """

        if u not in self.G: # bad op
            return self

        # invalidate SCCs
        self.cc = None

        if v is not None: # remove edge (u,v)
            if v in self.G[u]:
                self.G[u].remove(v)
        else: # no v supplied, remove all of u
            del self.G[u]
            for w in self.G:
                if u in self.G[w]:
                    self.G[w].remove(u)

        return self

    def _scc(
        self, 
        u: Any, 
        low: Dict[Any,int], 
        disc: Dict[Any,int], 
        mem: Dict[Any,bool], 
        st: List[Any]
    ) -> None:
        """(Internal) Recursively find Strongly Connected Components

        A recursive function that find finds and adds SCCs using DFS traversal. 

        Changes object state by updating self.cc when an SCC is found. 
        Uses self.t as an intermediate variable (but this could be passed?)

        u       The vertex to be visited next
        low     Earliest visited vertex (the vertex with minimum
                discovery time) that can be reached from subtree
                rooted with current vertex
        disc    Stores discovery times of visited vertices
        mem     array for faster check whether a node is in stack
        st      To store all the connected ancestors (could be part of SCC)
        """

        # initialize and increment discovery time
        disc[u], low[u], mem[u] = self.t, self.t, True
        st.append(u)
        self.t += 1

        # Go through all vertices adjacent to current vertex
        for v in self.G[u]:
            
            # If v is not visited yet, then recurse for it
            if disc[v] < 0:
            
                self._scc(v, low, disc, mem, st)

                # Check if the subtree rooted with v has a connection to
                # one of the ancestors of u
                # Case 1 (per above discussion on Disc and Low value)
                low[u] = min(low[u], low[v])
                        
            elif mem[v] == True:

                # Update low value of 'u' only if 'v' is still in stack
                # (i.e. it's a back edge, not cross edge).
                # Case 2 (per above discussion on Disc and Low value)
                low[u] = min(low[u], disc[v])

        # head node found, pop the stack and print an SCC
        w, comp = None, [] # To store stack extracted vertices
        if low[u] == disc[u]:
            while w != u:
                w = st.pop()
                comp.append(w)
                mem[w] = False
            self.cc.append(comp)
            
    def scc(self) -> List[List[Any]]:
        """ Find Strongly Connected Components (SCCs)

        "caches" the list of SCCs, up until nodes/edges change. 
        
        Initializes the following for the recursive method: 

        u       The vertex to be visited next
        low     Earliest visited vertex (the vertex with minimum
                discovery time) that can be reached from subtree
                rooted with current vertex
        disc    Stores discovery times of visited vertices
        mem     array for faster check whether a node is in stack
        st      To store all the connected ancestors (could be part of SCC)
        """

        # returned "cached" result if available (and not
        # invalidated by addition/removal)
        if self.cc is not None: 
            return self.cc

        self.t, self.cc = 0, [] # discovery time and connected components

        # Mark all the vertices as not visited
        # and Initialize parent and visited,
        # and ap(articulation point) arrays
        low  = {v: -1 for v in self.G}
        disc = {v: -1 for v in self.G}
        mem  = {v: False for v in self.G}
        st   = []
        
        # Call the recursive helper function rooting at v
        for v in self.G:
            if disc[v] < 0:
                self._scc(v, low, disc, mem, st)

        return self.cc

    def cyclic(self) -> bool:
        """ True if the graph is cyclic (has a cycle), False if otherwise 

        Uses SCCs: a graph is cyclic if and only if the number of SCCs is
        less than the number of nodes. 
        """
        self.scc()
        return len(self.cc) < len(self.G)

    def acyclic(self) -> bool:
        return not self.cyclic()

    def path(
        self, 
        u: Any, 
        v: Any, 
        head=[],
    ) -> Optional[List[Any]]: 
        """ Find a path between vertices

        Ad-hoc algorithm based on BFS with a prev-path check for
        cycles. Don't know complexity but can't be more than O(|N|+|E|)
        """

        if u is None or v is None: 
            return None

        if u not in self.G or v not in self.G:
            return None

        if v in self.G[u]:
            return [u,v]

        if u in head: # we've cycled
            return [u]

        # Can we cache/memoize this? Note it depends on the full path
        # approaching u, not just u. So caching over u alone is incorrect. 
        # caching over the prior path might though, but we would need to
        # know this situation could occur (which I don't think it could.)
        bfs = {w: self.path(w, v, head=head+[u]) for w in self.G[u]}

        p = None # presume no path
        for w in self.G[u]:
            if bfs[w] is None or len(bfs[w]) < 2:
                del bfs[w] # necessary for a local datastructure?
            else:
                if len(bfs[w]) == 2:
                    return [u] + bfs[w]
                elif p is None or len(bfs[w]) < len(p):
                    p = [u] + bfs[w]
        return p # returns None when u,v disconnected in G

