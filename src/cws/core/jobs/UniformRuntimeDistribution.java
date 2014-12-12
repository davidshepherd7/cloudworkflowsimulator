package cws.core.jobs;

import java.util.Random;

/**
 * Returns 'runtime' +/- 'variance' percent of 'runtime', where the actual
 * variance is drawn from a uniform distribution. e.g. if 'variance' is 0.1,
 * then it will return a random uniform value that is +/- 10% of 'runtime'.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class UniformRuntimeDistribution implements RuntimeDistribution {
    private Random random;
    private double variance;

    public UniformRuntimeDistribution(double variance) {
        this.random = new Random();
        this.variance = variance;
    }

    @Override
    public double getActualRuntime(double runtime) {
        // Get a random number in the range [-1,+1]
        double plusorminus = (random.nextDouble() * 2.0d) - 1.0d;
        return runtime + (plusorminus * variance * runtime);
    }

    @Override
    public double getVariance() {
        return variance;
    }

    public static void main(String[] args) {
        RuntimeDistribution d = new UniformRuntimeDistribution(0.20);

        for (int i = 0; i < 1000; i++) {
            System.out.println(d.getActualRuntime(100));
        }
    }
}
