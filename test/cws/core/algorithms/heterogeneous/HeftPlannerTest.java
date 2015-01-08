package cws.core.algorithms.heterogeneous;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.Rule;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.ArrayList;
import static java.util.Arrays.asList;
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

        Planner planner = new HeftPlanner();
        Plan actual = planner.planDAG(dag, new Plan(vms));

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

        Planner planner = new HeftPlanner();
        Plan actual = planner.planDAG(dag, new Plan(vms));

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

        Planner planner = new HeftPlanner();
        Plan actual = planner.planDAG(dag, new Plan(vms));

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
        Planner planner = new HeftPlanner();
        Plan actual = planner.planDAG(dag, new Plan(vms));

        assertSamePlans(actual, expected);
    }

    @Test
    public void testWithResourceTermination() throws NoFeasiblePlan {

        final double terminationTime = 5.34;

        final DAG dag = makeTasks();

        final VMType vmtype = VMTypeBuilder.newBuilder().mips(1).cores(1)
                .price(1).build();
        final Resource r1 = new Resource(vmtype, 0.0, terminationTime);
        final Resource r2 = new Resource(vmtype, terminationTime);
        Plan initialPlan = new Plan();
        initialPlan.resources.add(r1);
        initialPlan.resources.add(r2);

        // Check it
        Planner planner = new HeftPlanner();
        Plan actual = planner.planDAG(dag, initialPlan);

        // Scheduling outside of start/termination time will throw
        // exceptions so no need to assert that.

        assertAllTasksArePlanned(actual, dag);
        assertNoTasksOverlap(actual);
    }


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testHandleImpossiblePlan() throws NoFeasiblePlan {

        final double terminationTime = 5.34;

        final DAG dag = makeSingleTaskDAG();

        // VM is too slow to finish in time, so that the task is impossible
        final VMType vmtype = VMTypeBuilder.newBuilder().mips(0.1).cores(1)
                .price(1).build();
        final Resource r = new Resource(vmtype, 0.0, terminationTime);
        Plan initialPlan = new Plan();
        initialPlan.resources.add(r);

        Planner planner = new HeftPlanner();

        // Check that exception is thrown
        thrown.expect(NoFeasiblePlan.class);
        Plan plan = planner.planDAG(dag, initialPlan);
    }


    @Test
    public void testWithNewVMAllocation() throws NoFeasiblePlan {
        DAG dag = new DAG();
        dag.addTask(new Task("a", "", 2));
        dag.addTask(new Task("b", "", 2));
        dag.addTask(new Task("c", "", 2));
        // no edges so that two VMs can be used

        final VMType vmtype = makeVM(1.0);
        Plan initialPlan = new Plan();
        final Resource r = new Resource(vmtype, 0.0);
        initialPlan.resources.add(r);


        Planner planner = new HeftPlanner(asList(vmtype));
        Plan actual = planner.planDAG(dag, initialPlan);

        assertAllTasksArePlanned(actual, dag);
        assertNoTasksOverlap(actual);

        assertThat("some additional vm was allocated",
                actual.vmList().size(),
                greaterThan(initialPlan.vmList().size()));


        assertThat("enough additional vms were allocated",
                actual.vmList().size(), is(dag.numTasks()));

    }

        @Test
    public void testNoExcessVMAllocation() throws NoFeasiblePlan {
        DAG dag = new DAG();
        dag.addTask(new Task("a", "", 2));
        dag.addTask(new Task("b", "", 2));
        dag.addTask(new Task("c", "", 2));
        dag.addEdge("a", "b");

        final VMType vmtype = makeVM(1.0);
        Plan initialPlan = new Plan();
        final Resource r = new Resource(vmtype, 0.0);
        initialPlan.resources.add(r);


        Planner planner = new HeftPlanner(asList(vmtype));
        Plan actual = planner.planDAG(dag, initialPlan);

        assertAllTasksArePlanned(actual, dag);
        assertNoTasksOverlap(actual);

        assertThat("no excess vm was allocated",
                actual.vmList().size(), is(2));
    }
}
