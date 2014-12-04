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
 * Allocates up to the allowed amount of VMs, and schedules times when the
 * VMs must be switched off, then calls an underlying Planner to create the
 * plan.
 *
 * @author David Shepherd
 */
public class PowerCappedPlanner implements Planner {

    Planner underlyingPlanner;
    final private TreeMap<Double, Double> powerCapsAtTimes;

    /** Construct with constant power cap */
    public PowerCappedPlanner(double constantPowerCap) {
        this.powerCapsAtTimes = new TreeMap<Double, Double>();
        this.powerCapsAtTimes.put(0.0, constantPowerCap);

        // Hard code this for now
        this.underlyingPlanner = new HeftPlanner();
    }

    /** Construct with piecewise constant power cap */
    public PowerCappedPlanner(TreeMap<Double, Double> powerCapsAtTimes) {

        // Defensive copy
        this.powerCapsAtTimes = new TreeMap<Double, Double>(powerCapsAtTimes);

        // Hard code this for now
        this.underlyingPlanner = new HeftPlanner();
    }

    private static <T> T getOne(Collection<T> many) {
        return many.iterator().next();
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
        final VMType vmtypeToCreate = getOne(currentPlan.resources).vmtype;

        // For each time that the power cap changes make sure we are below
        // it, but not too far below.
        for (final Map.Entry<Double, Double> entry : powerCapsAtTimes.entrySet()) {
            final double time = entry.getKey();
            final double powerCap = entry.getValue();

            // Remove VMs until we get below the power cap
            removeVMsUntilCap(currentPlan, time, powerCap);

            // If we are below the cap then add some new VMs if we can
            addVMsUntilCap(currentPlan, time, powerCap, vmtypeToCreate);
        }

        // Clean out any Resources that start and stop at the same time
        cleanUpZeroTimeResources(currentPlan);

        return currentPlan;
    }

    private static void addVMsUntilCap(Plan plan, double time, double powerCap,
            VMType vmtype) {

        // Add VMs that start now until we are near the cap
        while (plan.powerConsumptionAt(time) + vmtype.powerConsumption < powerCap) {
            plan.resources.add(new Resource(vmtype, time));
        }
    }

    private static void removeVMsUntilCap(Plan plan, double time, double powerCap) {

        // ??ds this is a bit messy but I'm not sure how else to do it

        // Turn off VMs untill we are below the cap
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

        // Cut down to the VMs we can use under the power cap
        Plan powerCappedPlan = createPowerCappedInitialPlan(currentPlan);

        // Schedule within these restrictions using something else
        return underlyingPlanner.planDAG(dag, powerCappedPlan);
    }
}
