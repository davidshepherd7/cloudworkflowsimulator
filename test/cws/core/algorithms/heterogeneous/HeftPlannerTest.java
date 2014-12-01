package cws.core.algorithms.heterogeneous;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;


import cws.core.VM;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.VMFactory;
import cws.core.provisioner.ConstantDistribution;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;


import cws.core.dag.Task;
import cws.core.dag.DAG;


//??ds pull out some helper functions? eg. dag makers


// Based on https://github.com/mrocklin/heft/blob/master/heft/tests/test_core.py
public class HeftPlannerTest {

    @Before
    public void setUp() {
    }

    public VMType makeVM(double mips) {
        return VMTypeBuilder.newBuilder().mips(mips).
                cores(1).price(1.0)
                .provisioningTime(new ConstantDistribution(0.0))
                .deprovisioningTime(new ConstantDistribution(0.0))
                .build();
    }

    public Map<VMType, Integer> makeUniformVMS() {

        // 3 identical VMs
        Map<VMType, Integer> agents = new HashMap<>();
        agents.put(makeVM(1.0), 3);

        return agents;
    }

    public Map<VMType, Integer> makeNonUniformVMS() {

        // One each of three different VMs
        Map<VMType, Integer> agents = new HashMap<>();
        agents.put(makeVM(0.5), 1);
        agents.put(makeVM(0.25), 1);
        agents.put(makeVM(1.0), 1);

        return agents;
    }


    @Test
    public void testVMEquality() {
        VMType a = makeVM(2.0);
        VMType b = makeVM(2.0);
        VMType c = makeVM(3.0);

        Assert.assertTrue(a.equals(a));
        Assert.assertFalse(a.equals(b));
        Assert.assertFalse(a.equals(c));
    }

    @Test
    public void testWBar() {
        Task myTask = new Task("a", "", 1.0);

        assertThat(1.0,
                is(HeftPlanner.meanComputationTime(myTask, makeUniformVMS())));

        assertThat((2.0 + 4 + 1)/3, 
                is(HeftPlanner.meanComputationTime(myTask, makeNonUniformVMS())));

    }


    public DAG makeTasks() {
        // dag = {a: (c,),
        //        b: (d,),
        //        c: (e,),
        //        d: (e,),
        //        e: (f, g)}

        DAG dag = new DAG();
        dag.addTask(new Task("a", "", 1.0));
        dag.addTask(new Task("b", "", 1.1));
        dag.addTask(new Task("c", "", 1.0));
        dag.addTask(new Task("d", "", 1.1));
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
        Map<VMType, Integer> vms = makeUniformVMS();

        // Check that the final tasks only depend on themselves
        assertThat(HeftPlanner.upwardRank(tasks.getTaskById("f"), vms),
                is(HeftPlanner.meanComputationTime(tasks.getTaskById("f"), vms)));
                

        // Check for one task deeper into the dag (assuming that f and g
        // cost the same).
        double expectedRank = HeftPlanner.meanComputationTime(tasks.getTaskById("e"), vms)
                // + HeftPlanner.cBar(tasks.getTaskById("e"), tasks.getTaskById("g"))
                + HeftPlanner.upwardRank(tasks.getTaskById("g"), vms);

        assertThat(HeftPlanner.upwardRank(tasks.getTaskById("e"), vms), is(expectedRank));
    }

    @Test
    public void testRankedTasks() {
        DAG dag = makeTasks();

        // The correct result
        List<Task> expected = new ArrayList<Task>();
        expected.add(dag.getTaskById("b"));
        expected.add(dag.getTaskById("a"));
        expected.add(dag.getTaskById("d"));
        expected.add(dag.getTaskById("c"));
        expected.add(dag.getTaskById("e"));
        expected.add(dag.getTaskById("g"));
        expected.add(dag.getTaskById("f"));

        List<Task> actual = HeftPlanner.rankedTasks(dag, makeUniformVMS());
        assertThat(actual, is(expected));
    }



}
