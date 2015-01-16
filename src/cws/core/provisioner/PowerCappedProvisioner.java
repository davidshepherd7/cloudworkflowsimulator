package cws.core.provisioner;

import java.lang.Boolean;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import static java.util.Arrays.asList;
import static java.util.Collections.sort;
import static java.util.Collections.reverse;
import static java.util.Collections.unmodifiableCollection;

import cws.core.Cloud;
import cws.core.Provisioner;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;

import cws.core.VM;
import cws.core.VMFactory;
import cws.core.core.VMType;
import cws.core.core.PiecewiseConstantFunction;


/**
 * ??ds
 *
 */
public class PowerCappedProvisioner extends Provisioner {

    private final PiecewiseConstantFunction powerCapFunction;
    private final List<VMType> availableVMTypes;

    public PowerCappedProvisioner (CloudSimWrapper cloudsim,
            PiecewiseConstantFunction powerCapFunction,
            List<VMType> availableVMTypes) {
        super(cloudsim);

        if (availableVMTypes.size() < 1) {
            throw new IllegalArgumentException(
                    "Need at least one VMType available to be created.");
        }

        this.powerCapFunction = powerCapFunction;
        this.availableVMTypes = availableVMTypes;
    }

    @Override
    public void provisionResources(WorkflowEngine engine, Object eventData) {

        final double currentTime = engine.clock();
        final double powerCap = powerCapFunction.getValue(currentTime);

        // This function could be optimized a lot by simply calculating the
        // number of VMs to create/destroy outright, but I wanted to make
        // it easily extensible to the case where we have multiple VMTypes,
        // which will loops like this (I think).

        // Decide which VMs to kill and any new ones to create
        List<VM> vmsToKill = vmsToKill(getCloud().getAvailableVMs(), powerCap);
        List<VM> vmsStillRunning = new ArrayList<VM>(getCloud().getAvailableVMs());
        vmsStillRunning.removeAll(vmsToKill);
        List<VMType> vmsToStart = vmsToStart(vmsStillRunning, powerCap);

        // Do the actual termination and/or launches
        for(VM vm : vmsToKill) {
            terminateVM(vm);
        }
        for(VMType vmtype : vmsToStart) {
            VM vm = VMFactory.createVM(vmtype, getCloudsim());
            launchVM(vm);
        }

        // Submit event to trigger the next provisioning request when the
        // power cap next changes
        final Map.Entry<Double, Double> nextPowerChange =
                powerCapFunction.getNextJump(currentTime);

        // If it's null there are no more power changes
        if (nextPowerChange != null) {
            final double delay = nextPowerChange.getKey() - currentTime;
            getCloudsim().send(engine.getId(), engine.getId(), delay,
                    WorkflowEvent.PROVISIONING_REQUEST);
        }
    }


    private static List<VM> vmsToKill(Collection<VM> vmsActive,
            double powerCap) {

        // Get a copy of the VM list where the VMs are sorted by isFree(),
        // with free VMs first.
        List<VM> vms = new LinkedList<VM>(vmsActive);
        Comparator<VM> compare = new Comparator<VM>() {
                @Override
                public int compare(VM vm1, VM vm2) {
                    return Boolean.compare(vm1.isFree(), vm2.isFree());
                }
            };
        Collections.sort(vms, Collections.reverseOrder(compare));

        // Now remove VMs (in order so that free ones are gone first) until
        // we are complying with the cap.
        Iterator<VM> it = vms.iterator();
        List<VM> vmsToKill = new LinkedList<VM>();
        while (it.hasNext() && powerConsumption(vms) > powerCap) {
            VM vm = it.next();
            vmsToKill.add(vm);
            it.remove();
        }

        return vmsToKill;
    }


    private List<VMType> vmsToStart(Collection<VM> vmsActive, double powerCap) {

        // ??ds Create instances of the first vmtype only for now. Check
        // that all vmtypes are the same to avoid confusion in the future.
        if (availableVMTypes.size() > 1) {
            throw new RuntimeException("Multiple VMTypes given, only the first is currently used by PowerCappedProvisioner");
            }
        final VMType vmtypeToCreate = availableVMTypes.iterator().next();


        List<VMType> vmsToStart = new LinkedList<>();

        double baseConsumption = powerConsumption(vmsActive);
        while ((baseConsumption + powerConsumptionFromType(vmsToStart)
                + vmtypeToCreate.getPowerConsumption()) < powerCap) {
            vmsToStart.add(vmtypeToCreate);
        }

        return vmsToStart;
    }


    private static double powerConsumption(Collection<VM> vms) {
        double power = 0.0;
        for (VM vm : vms) {
            power += vm.getVmType().getPowerConsumption();
        }
        return power;
    }

    private static double powerConsumptionFromType(Collection<VMType> vms) {
        double power = 0.0;
        for (VMType vm : vms) {
            power += vm.getPowerConsumption();
        }
        return power;
    }
}
