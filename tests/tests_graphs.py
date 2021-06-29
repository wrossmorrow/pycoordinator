
from coordinator import Graph

def run_tests(g: Graph) -> None:
    """ Not really tests tests

    Print the group and it's SCCs (and whether cyclic)
    Then enumerate all paths and assert some basic properties
    of them: they start and end correctly, and any pair in the
    path is in fact an edge
    """
    print( g.nodes() , "->" , ', '.join([f"{l}" for l in g.scc()]) , f"({g.cyclic()})" )
    for n in g.nodes():
        for m in [m for m in g.nodes() if m != n]:
            p = g.path(n,m)
            if p is not None:
                assert p[0] == n
                assert p[-1] == m
                for i in range(1,len(p)):
                    assert g.is_edge(p[i-1], p[i])
                print(" ", n, "->", m, ":", ' -> '.join([f"{v}" for v in p]))

def test_1():
    g = Graph().add(1, 0).add(0, 2).add(2, 1).add(0, 3).add(3, 4)
    run_tests(g)
    run_tests(g.reverse())

def test_2():
    g = Graph(edges=[(1,0),(0,2),(2,1),(0,3),(3,4)])
    run_tests(g)
    run_tests(g.reverse())

def test_3():
    g = Graph().add(0, 1).add(1, 2).add(2, 3)
    run_tests(g)
    run_tests(g.reverse())

def test_4():
    g = Graph()
    g.add(0, 1)
    g.add(1, 2)
    g.add(2, 0)
    g.add(1, 3)
    g.add(1, 4)
    g.add(1, 6)
    g.add(3, 5)
    g.add(4, 5)
    run_tests(g)
    run_tests(g.reverse())

def test_5():
    g = Graph()
    g.add(0, 1)
    g.add(0, 3)
    g.add(1, 2)
    g.add(1, 4)
    g.add(2, 0)
    g.add(2, 6)
    g.add(3, 2)
    g.add(4, 5)
    g.add(4, 6)
    g.add(5, 6)
    g.add(5, 7)
    g.add(5, 8)
    g.add(5, 9)
    g.add(6, 4)
    g.add(7, 9)
    g.add(8, 9)
    g.add(9, 8)
    run_tests(g)
    run_tests(g.reverse())

def test_6():
    g = Graph(edges=[(0,1),(1,2),(2,3),(2,4),(3,0),(4,2)])
    run_tests(g)
    run_tests(g.reverse())
