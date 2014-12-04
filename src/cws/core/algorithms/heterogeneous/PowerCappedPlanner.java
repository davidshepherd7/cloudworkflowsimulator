package cws.core.algorithms.heterogeneous;

import java.util.Collections;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Random;


import org.cloudbus.cloudsim.Log;

import cws.core.VM;
import cws.core.core.VMType;

import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;

import cws.core.engine.Environment;
import cws.core.cloudsim.CloudSimWrapper;

import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;


/**
 * A planner for the case when we have a (known but possibly varying) cap
 * on the electrical power that can be used and so cannot run all VMs for
 * the entire duration of the workflow computation.
 *
 * Given a plan containing some VMs (a.k.a. Resources) schedules times when
 * the VMs must be switched off/on to meet the power cap. Then calls an
 * underlying Planner to create the final plan.
 *
 * @author David Shepherd
 */
public class PowerCappedPlanner implements Planner {

    /** The planner that does the real planning once we have decided which
     * Resources are active at which times */
    final private Planner underlyingPlanner;

    /** A representation of the piecewise constant power cap. The first
     * value represents the time of the change in power cap, and the second
     * value the power cap itself.
     */
    final private TreeMap<Double, Double> powerCapsAtTimes;

    /** Construct with constant power cap */
    public PowerCappedPlanner(double constantPowerCap, Planner planner) {
        this.powerCapsAtTimes = new TreeMap<Double, Double>();
        this.powerCapsAtTimes.put(0.0, constantPowerCap);
        this.underlyingPlanner = planner;
    }

    /** Construct with piecewise constant power cap */
    public PowerCappedPlanner(
            TreeMap<Double, Double> powerCapsAtTimes, Planner planner) {
        // Make Defensive copy
        this.powerCapsAtTimes = new TreeMap<Double, Double>(powerCapsAtTimes);
        this.underlyingPlanner = planner;
    }


    /** The function that makes sure we are ok for power. currentPlan
     * contains all the allowed VMs, this function removes or turns off
     * some to reach the power cap. */
    public Plan createPowerCappedInitialPlan(Plan currentPlan) {

        // Check that schedules are empty, since we are killing off VMs
        // this has to happen before any tasks are scheduled
        for (Resource r : currentPlan.resources) {
            if (r.getSlots().size() != 0) {
                throw new RuntimeException("Tried to run with tasks already scheduled");
            }
        }

        //??ds Create instances of a random vmtype... for now
        final VMType vmtypeToCreate = currentPlan.resources.iterator().next().vmtype;

        // For each time that the power cap changes make sure we are below
        // it, but not too far below.
        for (final Map.Entry<Double, Double> entry : powerCapsAtTimes.entrySet()) {
            final double time = entry.getKey();
            final double powerCap = entry.getValue();

            // Remove Resources until we get below the power cap
            removeResourcesUntilCap(currentPlan, time, powerCap);

            // If we are below the cap then add some new Resources if we can
            addResourcesUntilCap(currentPlan, time, powerCap, vmtypeToCreate);
        }

        // Clean out any Resources that start and stop at the same time
        cleanUpZeroTimeResources(currentPlan);

        return currentPlan;
    }


    private static void addResourcesUntilCap(Plan plan, double time, double powerCap,
            VMType vmtype) {

        // Add Resources that start now until we are near the cap
        while (plan.powerConsumptionAt(time) + vmtype.powerConsumption < powerCap) {
            plan.resources.add(new Resource(vmtype, time));
        }
    }


    private static void removeResourcesUntilCap(Plan plan, double time, double powerCap) {

        // ??ds this is a bit messy but I'm not sure how else to do it

        // Turn off Resources untill we are below the cap
        Set<Resource> terminatedResources = new HashSet<>();
        Iterator<Resource> it = plan.resources.iterator();
        while (it.hasNext() && plan.powerConsumptionAt(time) > powerCap) {
            Resource r = it.next();
            // If it is turned on now then replace it with one that is
            // terminated now (immutable so can't just set the
            // terminationTime).
            if (r.isOnAt(time)) {
                terminatedResources.add(new Resource(r.vmtype, r.startTime, time));
                it.remove();
            }
        }

        plan.resources.addAll(terminatedResources);
    }

    /** Remove Resources that are started and terminated at the same
     * time from the plan
     */
    public static void cleanUpZeroTimeResources(Plan plan) {
        Iterator<Resource> it = plan.resources.iterator();
        while (it.hasNext()) {
            Resource r = it.next();
            if (Math.abs(r.terminationTime - r.startTime) < 1e-12) {
                it.remove();
            }
        }
    }

    /** The main function of the Planner interface. */
    @Override
    public Plan planDAG(DAG dag, Plan currentPlan) throws NoFeasiblePlan {

        // Choose Resources to meet the power cap
        Plan powerCappedPlan = createPowerCappedInitialPlan(currentPlan);

        // Schedule within these restrictions using some other Planner
        return underlyingPlanner.planDAG(dag, powerCappedPlan);
    }
}
