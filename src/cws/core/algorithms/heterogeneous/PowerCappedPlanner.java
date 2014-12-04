package cws.core.algorithms.heterogeneous;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
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

    /** The function that makes sure we are ok for power. currentPlan
     * contains all the allowed VMs, this function removes or turns off
     * some to reach the power cap. */
    public Plan createPowerCappedInitialPlan(Plan currentPlan) {

        for(Map.Entry<Double, Double> entry : powerCapsAtTimes.entrySet()) {
            final double time = entry.getKey();
            final double powerCap = entry.getValue();

            // Turn off VMs till we are below the cap
            while(currentPlan.powerConsumptionAt(time) > powerCap) {
                Resource VMToKill = currentPlan.resources.iterator().next();
                currentPlan.resources.remove(VMToKill);
            }
        }

        return currentPlan;
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
