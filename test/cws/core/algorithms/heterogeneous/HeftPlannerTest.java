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
import java.util.Collections;
import java.util.Comparator;


import cws.core.VM;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.VMFactory;
import cws.core.provisioner.ConstantDistribution;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;


import cws.core.dag.Task;
import cws.core.dag.DAG;

import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;



public class HeftPlannerTest extends PlannerTestBase {

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

        List<Task> actual = HeftPlanner.rankedTasks(dag, makeUniformVMS(3));
        assertThat(actual, is(expected));
    }

    @Test
    public void testCreatePlan() throws NoFeasiblePlan {

        List<VMType> vms = makeUniformVMS(3);

        DAG dag = makeTasks();
        List<Task> ranked = HeftPlanner.rankedTasks(dag, vms);

        Plan actual = HeftPlanner.createPlan(ranked, new Plan(vms));

        Plan expected = new Plan();

        Resource r = new Resource(vms.get(0));
        expected.schedule(r, dag.getTaskById("b"), 0.0);
        expected.schedule(r, dag.getTaskById("d"), 1.1);
        expected.schedule(r, dag.getTaskById("e"), 2.2);
        expected.schedule(r, dag.getTaskById("g"), 3.2);

        Resource r2 = new Resource(vms.get(1));
        expected.schedule(r2, dag.getTaskById("a"), 0.0);
        expected.schedule(r2, dag.getTaskById("c"), 1.0);
        expected.schedule(r2, dag.getTaskById("f"), 3.2);

        // Resource with no jobs
        expected.resources.add(new Resource(vms.get(2)));

        assertSamePlans(actual, expected);
    }

    @Test
    public void testPlanWithInsertionAtStart() throws NoFeasiblePlan {
        List<VMType> vms = makeUniformVMS(2);

        // Make tasks
        // ============================================================
        DAG dag = new DAG();

        dag.addTask(new Task("a", "", 9));
        dag.addTask(new Task("b", "", 10));
        dag.addTask(new Task("c", "", 11));
        dag.addTask(new Task("d", "", 1));

        dag.addEdge("a", "b");
        dag.addEdge("a", "c");


        // Make expected plan
        // ============================================================

        Plan expected = new Plan();

        Resource r = new Resource(vms.get(0));
        expected.schedule(r, dag.getTaskById("a"), 0.0);
        expected.schedule(r, dag.getTaskById("c"), 9.0);

        Resource r2 = new Resource(vms.get(1));
        expected.schedule(r2, dag.getTaskById("b"), 9.0);
        expected.schedule(r2, dag.getTaskById("d"), 0.0);

        Plan actual = HeftPlanner.createPlan(HeftPlanner.rankedTasks(dag, vms), new Plan(vms));

        assertSamePlans(actual, expected);
    }

    @Test
    public void testPlanWithInsertionInMiddle() throws NoFeasiblePlan {
        List<VMType> vms = makeUniformVMS(2);

        // Make tasks
        // ============================================================

        DAG dag = new DAG();

        dag.addTask(new Task("a", "", 9));
        dag.addTask(new Task("b", "", 10));
        dag.addTask(new Task("c", "", 11));
        dag.addTask(new Task("d", "", 1));
        dag.addTask(new Task("e", "", 1));

        dag.addEdge("a", "b");
        dag.addEdge("a", "c");


        // Make expected plan
        // ============================================================

        Plan expected = new Plan();

        Resource r = new Resource(vms.get(0));
        expected.schedule(r, dag.getTaskById("a"), 0.0);
        expected.schedule(r, dag.getTaskById("c"), 9.0);

        Resource r2 = new Resource(vms.get(1));
        expected.schedule(r2, dag.getTaskById("b"), 9.0);
        expected.schedule(r2, dag.getTaskById("d"), 0.0);
        expected.schedule(r2, dag.getTaskById("e"), 1.0);

        Plan actual = HeftPlanner.createPlan(HeftPlanner.rankedTasks(dag, vms), new Plan(vms));

        assertSamePlans(actual, expected);
    }


    @Test
    public void testWithHeterogeneousVMs() throws NoFeasiblePlan {

        // Make the dag
        DAG dag = new DAG();
        dag.addTask(new Task("a", "", 20));
        dag.addTask(new Task("b", "", 10));
        dag.addTask(new Task("c", "", 20));
        dag.addTask(new Task("d", "", 1));
        dag.addTask(new Task("e", "", 5));

        dag.addEdge("a", "b");
        dag.addEdge("a", "c");
        dag.addEdge("b", "e");
        dag.addEdge("c", "d");

        // One fast one slow VM
        VMType fastVM = makeVM(10);
        VMType slowVM = makeVM(1);
        List<VMType> vms = new ArrayList<>();
        vms.add(fastVM);
        vms.add(slowVM);

        // Expected plan
        Plan expected = new Plan();
        Resource r = new Resource(fastVM);
        expected.schedule(r, dag.getTaskById("a"), 0);
        expected.schedule(r, dag.getTaskById("b"), 4);
        expected.schedule(r, dag.getTaskById("c"), 2);
        expected.schedule(r, dag.getTaskById("e"), 5);
        Resource rSlow = new Resource(slowVM);
        expected.schedule(rSlow, dag.getTaskById("d"), 4);

        // Check it
        Plan actual = HeftPlanner.createPlan(HeftPlanner.rankedTasks(dag, vms), new Plan(vms));
        assertSamePlans(actual, expected);
    }

}
