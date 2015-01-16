package cws.core.provisioner;

import static org.junit.Assert.assertThat;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.Rule;
import static org.mockito.Mockito.*;
import static org.mockito.AdditionalMatchers.gt;

import java.lang.Math;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.Collection;
import static java.util.Arrays.asList;
import static java.util.Collections.sort;
import static java.util.Collections.reverse;
import static java.util.Collections.unmodifiableCollection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Collections2;


import cws.core.Cloud;
import cws.core.Provisioner;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;


import cws.core.VM;
import cws.core.VMFactory;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.core.PiecewiseConstantFunction;


public class PowerCappedProvisionerTest {

    WorkflowEngine engine;
    Cloud cloud;
    CloudSimWrapper cloudsim;
    VMType vmtype;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        cloudsim = new CloudSimWrapper();
        cloudsim.init();

        engine = mock(WorkflowEngine.class);
        cloud = mock(Cloud.class);

        vmtype = makeVMType();
    }

    public VMType makeVMType() {
        return VMTypeBuilder.newBuilder().mips(1.5)
                .cores(1).price(1.0)
                .provisioningTime(new ConstantDistribution(0.0))
                .deprovisioningTime(new ConstantDistribution(0.0))
                .powerConsumptionInWatts(1.0)
                .build();
    }


    @Test
    public void testRequireAtLeastOneVMType() {
        thrown.expect(IllegalArgumentException.class);
        Provisioner a = new PowerCappedProvisioner(cloudsim,
                mock(PiecewiseConstantFunction.class),
                new ArrayList<VMType>());
    }


    @Test
    public void testInitialAllocation() {
        PiecewiseConstantFunction powerCap = new PiecewiseConstantFunction(0.0);
        powerCap.addJump(0.0, 3.1);

        when(engine.clock()).thenReturn(0.0);
        when(cloud.getAvailableVMs()).thenReturn(ImmutableList.<VM>of());

        Provisioner a = new PowerCappedProvisioner(cloudsim, powerCap, asList(vmtype));
        a.setCloud(cloud);
        a.provisionResources(engine);

        // Check that we launched the correct number of VMs: floor(3.1/1.0)
        verify(cloud, times(3)).launchVM(anyInt(), any(VM.class));
    }


    @Test
    public void testLaterAllocation() {
        PiecewiseConstantFunction powerCap = new PiecewiseConstantFunction(0.0);
        powerCap.addJump(0.0, 3.1);
        powerCap.addJump(5.0, 10.1);


        List<VM> threeVMs = ImmutableList.of(VMFactory.createVM(vmtype, cloudsim),
                VMFactory.createVM(vmtype, cloudsim),
                VMFactory.createVM(vmtype, cloudsim));


        when(engine.clock()).thenReturn(5.0);
        when(cloud.getAvailableVMs()).thenReturn(threeVMs);

        Provisioner a = new PowerCappedProvisioner(cloudsim, powerCap, asList(vmtype));
        a.setCloud(cloud);
        a.provisionResources(engine);

        // Check that we launched the correct number of VMs:
        // floor(10.1/1.0) - 3 = 7
        verify(cloud, times(7)).launchVM(anyInt(), any(VM.class));
    }


    @Test
    public void testDeallocation() {
        PiecewiseConstantFunction powerCap = new PiecewiseConstantFunction(0.0);
        powerCap.addJump(0.0, 3.1);
        powerCap.addJump(4.0, 1.1);

        List<VM> threeVMs = ImmutableList.of(VMFactory.createVM(vmtype, cloudsim),
                VMFactory.createVM(vmtype, cloudsim),
                VMFactory.createVM(vmtype, cloudsim));

        when(engine.clock()).thenReturn(4.0);
        when(cloud.getAvailableVMs()).thenReturn(threeVMs);

        Provisioner a = new PowerCappedProvisioner(cloudsim, powerCap, asList(vmtype));
        a.setCloud(cloud);
        a.provisionResources(engine);

        // Check that we terminated the correct number of VMs: 3 -
        // floor(1.1/1.0) = 2
        verify(cloud, times(2)).terminateVM(any(VM.class));
    }


    @Test
    public void testFavourDeallocatingFreeVMs() {
        PiecewiseConstantFunction powerCap = new PiecewiseConstantFunction(0.0);
        powerCap.addJump(0.0, 3.1);
        powerCap.addJump(4.0, 1.1);


        // Make a busy VM mock
        VM busyVM = mock(VM.class);
        when(busyVM.isFree()).thenReturn(false);
        when(busyVM.getVmType()).thenReturn(vmtype);

        List<VM> threeVMs = ImmutableList.of(busyVM,
                VMFactory.createVM(vmtype, cloudsim),
                VMFactory.createVM(vmtype, cloudsim));

        when(engine.clock()).thenReturn(4.0);

        // Check it with each possible order of the list
        for(List<VM> permutation : Collections2.permutations(threeVMs)) {

            when(cloud.getAvailableVMs()).thenReturn(permutation);

            Provisioner a = new PowerCappedProvisioner(cloudsim, powerCap, asList(vmtype));
            a.setCloud(cloud);
            a.provisionResources(engine);

            // Check that we left the busy VM alone
            verify(cloud, never()).terminateVM(busyVM);
        }

        // Make sure we did actually terminate some VMs!
        verify(cloud, atLeast(1)).terminateVM(any(VM.class));
    }

    @Test
    public void testDeallocateAllVMs() {
        PiecewiseConstantFunction powerCap = new PiecewiseConstantFunction(0.0);
        powerCap.addJump(0.0, 3.1);
        powerCap.addJump(4.0, 0.5);

        // Make a busy VM mock
        VM busyVM = mock(VM.class);
        when(busyVM.isFree()).thenReturn(false);
        when(busyVM.getVmType()).thenReturn(vmtype);

        List<VM> threeVMs = ImmutableList.of(busyVM,
                VMFactory.createVM(vmtype, cloudsim),
                VMFactory.createVM(vmtype, cloudsim));

        when(engine.clock()).thenReturn(4.0);
        when(cloud.getAvailableVMs()).thenReturn(threeVMs);

        Provisioner a = new PowerCappedProvisioner(cloudsim, powerCap, asList(vmtype));
        a.setCloud(cloud);
        a.provisionResources(engine);

        // Check that we terminated all 3, even the busy one
        verify(cloud, times(3)).terminateVM(any(VM.class));
        verify(cloud, times(1)).terminateVM(busyVM);
    }

    @Test
    public void testSendNextProvisioningRequest() {
        CloudSimWrapper cloudsimMock = mock(CloudSimWrapper.class);

        // Note that all the power cap times are powers of 2 to avoid
        // floating point issues. Floating point issuses are in a separate
        // test.

        PiecewiseConstantFunction powerCap = new PiecewiseConstantFunction(0.0);
        powerCap.addJump(0.0, 3.1);
        powerCap.addJump(4.0, 0.5);
        powerCap.addJump(16.0, 2.5);


        when(cloud.getAvailableVMs()).thenReturn(ImmutableList.<VM>of());

        Provisioner a = new PowerCappedProvisioner(cloudsimMock, powerCap, asList(vmtype));
        a.setCloud(cloud);

        when(engine.clock()).thenReturn(0.0);
        a.provisionResources(engine);

        when(engine.clock()).thenReturn(4.0);
        a.provisionResources(engine);

        // Send request for provisioning at 4.0
        verify(cloudsimMock, times(1)).send(anyInt(), anyInt(),
                eq(4.0), eq(WorkflowEvent.PROVISIONING_REQUEST));

        // Then at 12.0
        verify(cloudsimMock, times(1)).send(anyInt(), anyInt(),
                eq(12.0), eq(WorkflowEvent.PROVISIONING_REQUEST));

        // Don't send any requests for any future provisioning
        verify(cloudsimMock, times(2)).send(anyInt(), anyInt(),
                anyDouble(), eq(WorkflowEvent.PROVISIONING_REQUEST));
    }

}
