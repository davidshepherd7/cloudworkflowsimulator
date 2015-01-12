package cws.core.provisioner;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;

/**
 * ??ds
 */
public abstract class HomogeneousCloudAwareProvisioner extends CloudAwareProvisioner {

    protected Environment environment;

    public HomogeneousCloudAwareProvisioner (CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
