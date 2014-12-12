package cws.core.jobs;

import cws.core.jobs.RuntimeDistribution;
import cws.core.jobs.IdentityRuntimeDistribution;
import cws.core.jobs.UniformRuntimeDistribution;
    
import cws.core.exception.IllegalCWSArgumentException;


/**
 * Create a runtime distribution.
 *
 * @author David Shepherd
 */
public class RuntimeDistributionFactory {
    
    private RuntimeDistributionFactory () {}
    
    static public RuntimeDistribution build(String distributionType,
            double variance) {

        String dt = distributionType.toLowerCase();
        
        if (dt == "identity") {
            if (variance != 0.0) {
                throw new IllegalCWSArgumentException(
                        "Can't have an identity distribution with non-zero variance");
            }
            return new IdentityRuntimeDistribution();
            
        } else if(dt == "uniform") {
            return new UniformRuntimeDistribution(variance);
            
        } else {
            throw new IllegalCWSArgumentException(
                    "Unrecognised distribution " + distributionType);
        }
    }
}
