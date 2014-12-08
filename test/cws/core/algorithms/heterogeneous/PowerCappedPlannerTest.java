package cws.core.algorithms.heterogeneous;


import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.Rule;

import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import java.util.SortedSet;
import java.util.TreeMap;

import org.cloudbus.cloudsim.core.CloudSim;
import cws.core.Cloud;
import cws.core.EnsembleManager;

import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMFactory;
import cws.core.VMListener;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.algorithms.AlgorithmStatistics;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.jobs.Job;
import cws.core.jobs.Job.Result;
import cws.core.jobs.JobListener;
import cws.core.provisioner.ConstantDistribution;


import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;


public class PowerCappedPlannerTest {

    VMType makeVmType(double power) {
        return VMTypeBuilder.newBuilder().mips(1.0).
                cores(1).price(1.0)
                .powerConsumptionInWatts(power)
                .provisioningTime(new ConstantDistribution(0.0))
                .deprovisioningTime(new ConstantDistribution(0.0))
                .build();
    }

    @Test
    public void testScheduleBelowConstantPowerCap() {

        // Initial plan over the cap
        VMType vmType = makeVmType(50);
        Plan initialPlan = new Plan(asList(vmType, vmType, vmType));

        final double powerCap = 112; // 2 vms
        PowerCappedPlanner planner =
                new PowerCappedPlanner(powerCap, null);

        // Create cut down initial plan and check it
        Plan plan = planner.createPowerCappedInitialPlan(initialPlan);

        assertThat("Total power is less than cap.",
                plan.powerConsumptionAt(0.1),
                lessThanOrEqualTo(powerCap));
    }

    @Test
    public void testScheduleBelowConstantTreeMapPowerCap() {

        // Initial plan over the cap
        VMType vmType = makeVmType(50);
        Plan initialPlan = new Plan(asList(vmType, vmType, vmType));

        PiecewiseConstantFunction powerCap = new PiecewiseConstantFunction();
        powerCap.addJump(0.0, 112.0); // 2 vms
        PowerCappedPlanner planner =
                new PowerCappedPlanner(powerCap, null);

        // Create cut down initial plan and check it
        Plan plan = planner.createPowerCappedInitialPlan(initialPlan);

        assertThat("Total power is less than cap.",
                plan.powerConsumptionAt(0.0),
                lessThanOrEqualTo(powerCap.getValue(0.0)));
    }


    @Test
    public void testScheduleBelowNonConstantPowerCap() {

        // Initial plan over the cap
        VMType vmType = makeVmType(50);
        Plan initialPlan = new Plan(asList(vmType, vmType, vmType));

        PiecewiseConstantFunction powerCap = new PiecewiseConstantFunction();
        powerCap.addJump(0.0, 101.0); // 2 vms
        powerCap.addJump(10.0, 51.0); // 1 vm
        powerCap.addJump(20.0, 201.0); // 4 vms

        PowerCappedPlanner planner =
                new PowerCappedPlanner(powerCap, null);

        // Create cut down initial plan and check it
        Plan plan = planner.createPowerCappedInitialPlan(initialPlan);

        // Check cap for all times
        for (Map.Entry<Double, Double> entry : powerCap) {
            final double time = entry.getKey();
            final double powerCapValue = entry.getValue();

            assertThat("Total power is less than cap.",
                    plan.powerConsumptionAt(time), lessThanOrEqualTo(powerCapValue));
        }

        assertThat("Initial power usage is not constrained by min power usage",
                plan.powerConsumptionAt(0.0),
                greaterThan(min(powerCap.values())));

        assertThat("Final power usage is not constrained by min power usage",
                plan.powerConsumptionAt(30),
                greaterThan(min(powerCap.values())));

    }

    @Test
    public void testRemoveResourcesWhichAreNeverOn() {
        VMType vmType = VMTypeBuilder.newBuilder().mips(1.0)
                .cores(1).price(1).build();

        Plan plan = new Plan();
        plan.resources.add(new Resource(vmType, 0.0, 0.0));
        plan.resources.add(new Resource(vmType, 35.0, 35.0));
        plan.resources.add(new Resource(vmType, 62.0, 62.0 + 1e-14));

        plan.cleanUpZeroTimeResources();

        assertThat("All resources which are turned on and off at the same time are removed.",
                plan.resources.size(), is(0));


        Plan plan2 = new Plan();
        plan2.resources.add(new Resource(vmType, 0.0, 0.0));
        plan2.resources.add(new Resource(vmType, 0.0, 0.1));
        plan2.resources.add(new Resource(vmType));

        plan2.cleanUpZeroTimeResources();

        assertThat("Other resources are left alone.",
                plan2.resources.size(), is(2));
    }


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testThrowIfNotEnoughPowerForAnyResource() throws NoFeasiblePlan {
        
        // Initial plan over the cap
        VMType vmType = makeVmType(50);
        Plan initialPlan = new Plan(asList(vmType));

        PowerCappedPlanner planner = new PowerCappedPlanner(0.1, null);

        DAG dag = new DAG();
        dag.addTask(new Task("a", "", 10));

        thrown.expect(NoFeasiblePlan.class);
        planner.planDAG(dag, initialPlan);
    }

}
