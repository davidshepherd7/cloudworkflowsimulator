package cws.core.algorithms;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.Collection;

import cws.core.core.VMType;
import cws.core.dag.Task;


/**
 * A collection of struct-like classes to help with constructing plans for
 * static scheduling.
 */
public class Plan {
    public LinkedHashSet<Resource> resources;

    public Plan() {
        this.resources = new LinkedHashSet<Resource>();
    }

    public Plan(Plan other) {
        this.resources = new LinkedHashSet<Resource>(other.resources);
    }

    public Plan(Collection<VMType> vms) {
        this.resources = new LinkedHashSet<Resource>();
        for(VMType vmt : vms) {
            resources.add(new Resource(vmt));
        }
    }

    // Make a list of vms available
    public List<VMType> vmList() {
        List<VMType> vms = new ArrayList<>();
        for (Resource r : resources) {
            vms.add(r.vmtype);
        }
        return vms;
    }

    public double getCost() {
        double cost = 0.0;
        for (Resource r : resources) {
            cost += r.getCost();
        }
        return cost;
    }

    public String toString() {
        return resources.toString();
    }


    /** Easy way to add to schedule */
    public void schedule(Resource r, Task t, double start) {
        if(!resources.contains(r)) {
            resources.add(r);
        }

        final double duration = r.vmtype.getPredictedTaskRuntime(t);

        r.addToSchedule(new Slot(t, start, duration));
    }

    /** Get the time a task finishes in the schedule*/
    public double getFinishTime(Task t) {
        //??ds optimise?

        for(Resource r : resources) {
            for(Map.Entry<Double, Slot> entry : r.schedule.entrySet()) {
                Slot s = entry.getValue();
                if(s.task.equals(t)) {
                    return s.start + s.duration;
                }
            }
        }

        throw new RuntimeException("Task not in plan.");
    }

    public double powerConsumptionAt(double time) {
        double totalPower = 0.0;

        for(Resource r : resources) {
            if((time >= r.startTime) && (time <= r.terminationTime)) {
                totalPower += r.vmtype.powerConsumption;
            }
        }

        return totalPower;
    }


    /** Class to group together the start and duration of a Task in a
     * Plan.
     */
    public static class Slot {
        final public Task task;
        final public double start;
        final public double duration;

        public Slot(Task task, double start, double duration) {
            this.task = task;
            this.start = start;
            this.duration = duration;
        }

        public String toString() {
            return String.format("Slot(%s, start: %f, duration: %f)",
                    task.toString(), start, duration);
        }
    }

    /** Class to group a vmtype (which will be created by the Algorithm
     * class when the plan is executed) with a schedule for the tasks
     * assigned to that VM.
     */
    public static class Resource {

        public VMType vmtype;

        private TreeMap<Double, Slot> schedule;

        // Times to start and stop the VM.
        final public double startTime;
        final public double terminationTime;

        public Resource(Resource other) {
            this(other.vmtype, other.startTime, other.terminationTime);
            this.schedule = new TreeMap<>(other.schedule);
        }

        public Resource(VMType vmtype) {
            this(vmtype, 0.0);
        }

        public Resource(VMType vmtype, double startTime) {
            this(vmtype, startTime, Double.MAX_VALUE);
        }

        public Resource(VMType vmtype, double startTime, double terminationTime) {
            this.vmtype = vmtype;
            this.schedule = new TreeMap<Double, Slot>();
            this.startTime = startTime;
            this.terminationTime = terminationTime;
        }

        public SortedSet<Double> getStartTimes() {
            return schedule.navigableKeySet();
        }

        /** Get an ordered collection of the slots in the schedule (in
         * order of start time).
         */
        public Collection<Slot> getSlots() {
            return schedule.values();
        }

        public void addToSchedule(Slot slot) {

            // Check that the slot is within the times that the VM is active
            if(slot.start < startTime) {
                throw new RuntimeException(
                        "Tried to schedule a task before VM will be ready");
            }
            if(slot.start + slot.duration > terminationTime) {
                throw new RuntimeException(
                        "Tried to schedule a task after VM will be terminated");
            }

            // Schedule it
            Slot previous = schedule.put(slot.start, slot);

            // Check that we didn't just stomp another slot
            if(previous != null) {
                throw new RuntimeException("Tried to overwrite slot at time "
                        + Double.toString(previous.start));
            }
        }

        /** Find the time when the first gap of size desiredGapDuration is
         * available. Trys both before all slots have started and after all
         * slots have finished, as well as in between all scheduled slots.
         */
        public Double findFirstGap(double desiredGapDuration) {

            // Try to fit it in before any of the slots start.
            if (schedule.size() == 0 ||
                    (schedule.firstKey() - startTime) > desiredGapDuration) {
                return startTime;
            }

            // Try to fit in between all the other pairs of slots
            for (final Slot s : getSlots()) {
                final double finishTime = s.start + s.duration;

                // Get the start time of the next slot, null if there is no
                // next slot.
                final Double nextStart = schedule.higherKey(s.start);

                if(nextStart != null
                        && (nextStart - finishTime) > desiredGapDuration) {
                    return finishTime;
                }
            }

            // Finally try to fit it in at the end
            final double startOfLast = schedule.lastKey();
            final double endOfLast = startOfLast + schedule.get(startOfLast).duration;
            if ((terminationTime - endOfLast) > desiredGapDuration) {
                return endOfLast;
            } else {
                // We failed, no slots available
                return null;
            }
        }

        public double getStart() {
            if (schedule.size() == 0) {
                return startTime;
            }
            return schedule.firstKey();
        }

        public double getEndOfSchedule() {
            if (schedule.size() == 0) {
                return startTime;
            }
            double last = schedule.lastKey();
            Slot lastSlot = schedule.get(last);
            return last + lastSlot.duration;
        }

        public double getEnd() {
            return getEndOfSchedule() + vmtype.getDeprovisioningDelayEstimation();
        }



        public int getFullBillingUnits() {
            return getFullBillingUnitsWith(getStart(), getEnd());
        }

        public int getFullBillingUnitsWith(double start, double end) {
            double seconds = end - start;
            double units = seconds / vmtype.getBillingTimeInSeconds();
            int rounded = (int) Math.ceil(units);
            return Math.max(1, rounded);
        }

        public double getCostWith(double start, double end) {
            return vmtype.getVMCostFor(end - start);
        }

        public double getCost() {
            return getCostWith(getStart(), getEnd());
        }

        public double getUtilization() {
            double runtime = 0.0;
            for (Slot sl : schedule.values()) {
                runtime += sl.duration;
            }
            return runtime / (getFullBillingUnits() * vmtype.getBillingTimeInSeconds());
        }



        public String toString() {
            return "Resource(" + vmtype.toString()
                    + ", start time = " + startTime
                    + ", termination time = " + terminationTime
                    + ", schedule = " + schedule.toString() + ")";
        }
    }

    /** Class to group together a slot with it's cost and a Resource
     * for comparison purposes.
     */
    public static class Solution {
        final double cost;
        final Resource resource;
        final Slot slot;
        final boolean newresource;

        public Solution(Resource resource, Slot slot, double cost, boolean newresource) {
            this.resource = resource;
            this.slot = slot;
            this.cost = cost;
            this.newresource = newresource;
        }

        public boolean betterThan(Solution other) {
            // A solution is better than no solution
            if (other == null) {
                return true;
            }

            // Cheaper solutions are better
            if (this.cost < other.cost) {
                return true;
            }
            if (this.cost > other.cost) {
                return false;
            }

            // Existing resources are better
            if (!this.newresource && other.newresource) {
                return true;
            }
            if (this.newresource && !other.newresource) {
                return false;
            }

            // Earlier starts are better
            if (this.slot.start < other.slot.start) {
                return true;
            }
            if (this.slot.start > other.slot.start) {
                return false;
            }

            return true;
        }

        public void addToPlan(Plan p) {
            resource.addToSchedule(slot);
            p.resources.add(resource);
        }

        public String toString() {
            return String.format("Solution(%s, start = %f, duration = %f)",
                    resource.vmtype.toString(),
                    slot.start,
                    slot.duration);
        }
    }

    public static class NoFeasiblePlan extends Exception {
        private static final long serialVersionUID = 1L;

        public NoFeasiblePlan(String msg) {
            super(msg);
        }
    }

}
