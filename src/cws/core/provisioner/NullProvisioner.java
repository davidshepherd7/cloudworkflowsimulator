package cws.core.provisioner;


import cws.core.Provisioner;
import cws.core.WorkflowEngine;


/**
 * A provisioner that will never provision anything.
 *
 * @author David Shepherd
 */
public class NullProvisioner implements Provisioner {

    public NullProvisioner() {}

    @Override
    public void provisionResources(WorkflowEngine engine) {
        // Don't provision anything
    }
}
