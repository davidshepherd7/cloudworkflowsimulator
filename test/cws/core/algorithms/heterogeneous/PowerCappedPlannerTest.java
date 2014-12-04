package cws.core.algorithms.heterogeneous;


import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import static java.util.Arrays.asList;
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

    @Test
    public void testScheduleBelowConstantPowerCap() {

        VMType vmType = VMTypeBuilder.newBuilder().mips(1.0).
                cores(1).price(1.0)
                .powerConsumptionInWatts(50)
                .provisioningTime(new ConstantDistribution(0.0))
                .deprovisioningTime(new ConstantDistribution(0.0))
                .build();

        // Initial plan over the cap
        Plan initialPlan = new Plan(asList(vmType, vmType, vmType));

        final double powerCap = 112;
        PowerCappedPlanner planner = new PowerCappedPlanner(powerCap);

        // Create cut down initial plan and check it
        Plan plan = planner.createPowerCappedInitialPlan(initialPlan);
        assertThat("Total power is less than cap.",
                plan.powerConsumptionAt(0.0), lessThanOrEqualTo(powerCap));
    }


}
