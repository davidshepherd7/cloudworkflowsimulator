package cws.core.algorithms;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import java.util.List;
import java.util.ArrayList;

import cws.core.VM;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.VMFactory;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;


import cws.core.dag.Task;
import cws.core.dag.DAG;




// Based on https://github.com/mrocklin/heft/blob/master/heft/tests/test_core.py
public class HeftTest {

    private CloudSimWrapper cloudsim;
    private Environment environment;

    public double commCost(Task j1, Task j2, VM A, VM B) {
        if(A.equals(B)) {
            return 0.0;
        } else {
            return 6.0;
        }
    }

    @Before
    public void setUp() {
        cloudsim = new CloudSimWrapper();
        cloudsim.init();

        environment = mock(Environment.class);
    }

    public VM makeVM(double mips) {
        VMType vmType = VMTypeBuilder.newBuilder().mips(mips).
            cores(1).price(1.0).build();
        return VMFactory.createVM(vmType, cloudsim);
    }

    public List<VM> makeUniformVMS() {
        List<VM> agents = new ArrayList<VM>();
        agents.add(makeVM(1.0));
        agents.add(makeVM(1.0));
        agents.add(makeVM(1.0));

        return agents;
    }

    public List<VM> makeNonUniformVMS() {
        List<VM> agents = new ArrayList<VM>();
        agents.add(makeVM(0.5));
        agents.add(makeVM(0.25));
        agents.add(makeVM(1.0));

        return agents;
    }



    @Test
    public void testVMEquality() {
        VM a = makeVM(2.0);
        VM b = makeVM(2.0);
        VM c = makeVM(3.0);

        Assert.assertTrue(a.equals(a));
        Assert.assertFalse(a.equals(b));
        Assert.assertFalse(a.equals(c));
    }

    @Test
    public void testWBar() {
        Task myTask = new Task("a", "", 1.0);

        Heft heftUniform = new Heft(0, 0, environment, cloudsim, makeUniformVMS());
        Assert.assertEquals(1.0, heftUniform.wBar(myTask));


        Heft heftNonUniform = new Heft(0, 0, environment, cloudsim, makeNonUniformVMS());
        Assert.assertEquals((2.0 + 4 + 1)/3, heftNonUniform.wBar(myTask));

    }

    @Test
    public void testCBar() {
        // Not implemented yet (needs storage aware stuff)
    }


    public DAG makeTasks() {
        // dag = {a: (c,),
        //        b: (d,),
        //        c: (e,),
        //        d: (e,),
        //        e: (f, g)}

        DAG dag = new DAG();
        dag.addTask(new Task("a", "", 1.0));
        dag.addTask(new Task("b", "", 1.0));
        dag.addTask(new Task("c", "", 1.0));
        dag.addTask(new Task("d", "", 1.0));
        dag.addTask(new Task("e", "", 1.0));
        dag.addTask(new Task("f", "", 1.0));
        dag.addTask(new Task("g", "", 1.5)); // break the f vs g symmetry

        dag.addEdge("a", "c");
        dag.addEdge("b", "d");
        dag.addEdge("c", "e");
        dag.addEdge("d", "e");
        dag.addEdge("e", "f");
        dag.addEdge("e", "g");

        return dag;
    }

    @Test
    public void testRankU() {

        DAG tasks = makeTasks();
        Heft heft = new Heft(0, 0, environment, cloudsim, makeUniformVMS());


        // Check that the final tasks only depend on themselves
        Assert.assertEquals(heft.wBar(tasks.getTaskById("f")),
                            heft.rankU(tasks.getTaskById("f")));

        // Check for one task deeper into the dag (assuming that f and g
        // cost the same).
        double expectedRank = heft.wBar(tasks.getTaskById("e"))
            // + heft.cBar(tasks.getTaskById("e"), tasks.getTaskById("g"))
            + heft.rankU(tasks.getTaskById("g"));

        Assert.assertEquals(expectedRank,
                            heft.rankU(tasks.getTaskById("e")));

    }

    @Test
    public void testRankedTasks() {
        DAG dag = makeTasks();
        Heft heft = new Heft(0, 0, environment, cloudsim, makeUniformVMS());

        // The correct result
        List<Task> expected = new ArrayList<Task>();
        expected.add(dag.getTaskById("b"));
        expected.add(dag.getTaskById("a"));
        expected.add(dag.getTaskById("d"));
        expected.add(dag.getTaskById("c"));
        expected.add(dag.getTaskById("e"));
        expected.add(dag.getTaskById("g"));
        expected.add(dag.getTaskById("f"));

        List<Task> actual = heft.rankedTasks(dag);
        Assert.assertEquals(expected, actual);
    }



}
