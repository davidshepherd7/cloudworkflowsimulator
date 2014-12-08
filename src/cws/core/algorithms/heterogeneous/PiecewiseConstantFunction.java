package cws.core.algorithms.heterogeneous;

import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Collection;
import java.util.NoSuchElementException;
import static java.util.Collections.unmodifiableCollection;

/**
 * A representation of a piecewise constant function using a map. The first
 * value represents the time of the jump, and the second value of the
 * function.
 *
 * The value at a jump time is defined to always be equal to the value just
 * after the jump time.
 *
 * @author David Shepherd
 */
public class PiecewiseConstantFunction implements Iterable<Map.Entry<Double, Double>> {

    /** Storage for the map from jump times to values */
    private TreeMap<Double, Double> jumpTimesAndValues;

    public PiecewiseConstantFunction(PiecewiseConstantFunction other) {
        this(other.jumpTimesAndValues);
    }

    public PiecewiseConstantFunction(TreeMap<Double, Double> jumpTimesAndValues) {
        this.jumpTimesAndValues = new TreeMap<>(jumpTimesAndValues);
    }

    public PiecewiseConstantFunction() {
        this(new TreeMap<Double, Double>());
    }

    public PiecewiseConstantFunction(double initialTime, double initialValue) {
        this();
        this.addJump(initialTime, initialValue);
    }

    /** Evaluate the function */
    public double getValue(double time) {
        Map.Entry<Double, Double> entry = jumpTimesAndValues.floorEntry(time);
        if (entry == null) {
            throw new NoSuchElementException();
        } else {
            return entry.getValue();
        }
    }

    /** Set up the function */
    public void addJump(double time, double value) {
        jumpTimesAndValues.put(time, value);
    }

    /** Iterate over the values */
    public Collection<Double> values() {
        return unmodifiableCollection(jumpTimesAndValues.values());
    }

    /** Iterate over jump times and values */
    @Override
    public Iterator<Map.Entry<Double, Double>> iterator() {
        return unmodifiableCollection(jumpTimesAndValues.entrySet()).iterator();
    }

    @Override
    public String toString() {
        return jumpTimesAndValues.toString();
    }

}
