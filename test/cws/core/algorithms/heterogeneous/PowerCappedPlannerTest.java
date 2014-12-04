package cws.core.algorithms.heterogeneous;


import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;
import org.junit.Before;
import org.junit.Test;

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
        PowerCappedPlanner planner = new PowerCappedPlanner(powerCap);

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

        TreeMap<Double, Double> powerCapsAtTimes = new TreeMap<>();
        powerCapsAtTimes.put(0.0, 112.0); // 2 vms
        PowerCappedPlanner planner = new PowerCappedPlanner(powerCapsAtTimes);

        // Create cut down initial plan and check it
        Plan plan = planner.createPowerCappedInitialPlan(initialPlan);

        assertThat("Total power is less than cap.",
                plan.powerConsumptionAt(0.0),
                lessThanOrEqualTo(powerCapsAtTimes.get(0.0)));
    }


    @Test
    public void testScheduleBelowNonConstantPowerCap() {

        // Initial plan over the cap
        VMType vmType = makeVmType(50);
        Plan initialPlan = new Plan(asList(vmType, vmType, vmType));

        TreeMap<Double, Double> powerCapsAtTimes = new TreeMap<>();
        powerCapsAtTimes.put(0.0, 101.0); // 2 vms
        powerCapsAtTimes.put(10.0, 51.0); // 1 vm
        powerCapsAtTimes.put(20.0, 201.0); // 4 vms

        PowerCappedPlanner planner = new PowerCappedPlanner(powerCapsAtTimes);

        // Create cut down initial plan and check it
        Plan plan = planner.createPowerCappedInitialPlan(initialPlan);

        // Check cap for all times
        for (Map.Entry<Double, Double> entry : powerCapsAtTimes.entrySet()) {
            final double time = entry.getKey();
            final double powerCap = entry.getValue();

            assertThat("Total power is less than cap.",
                    plan.powerConsumptionAt(time), lessThanOrEqualTo(powerCap));
        }

        assertThat("Initial power usage is not constrained by min power usage",
                plan.powerConsumptionAt(0.0),
                greaterThan(min(powerCapsAtTimes.values())));

        assertThat("Final power usage is not constrained by min power usage",
                plan.powerConsumptionAt(30),
                greaterThan(min(powerCapsAtTimes.values())));

    }

}
