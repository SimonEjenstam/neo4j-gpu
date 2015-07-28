package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;

/**
 * Created by simon.evertsson on 2015-07-27.
 */
public class PossibleSolutions {

    private CLBuffer<Integer> solutionElements;

    private CLBuffer<Integer> solutionRelationships;

    public PossibleSolutions(CLBuffer<Integer> solutionElements, CLBuffer<Integer> solutionRelationships) {
        this.solutionElements = solutionElements;
        this.solutionRelationships = solutionRelationships;
    }

    public CLBuffer<Integer> getSolutionElements() {
        return solutionElements;
    }

    public CLBuffer<Integer> getSolutionRelationships() {
        return solutionRelationships;
    }
}
