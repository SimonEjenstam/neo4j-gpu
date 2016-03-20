package se.simonevertsson.gpu.kernel;

import com.nativelibs4java.opencl.CLContext;
import com.nativelibs4java.opencl.CLPlatform;
import com.nativelibs4java.opencl.CLQueue;
import com.nativelibs4java.opencl.JavaCL;
import se.simonevertsson.*;

import java.io.IOException;

public class QueryKernels {
    public CLContext context;
    public CLQueue queue;
    public CheckCandidates checkCandidatesKernel;
    public ExploreCandidates exploreCandidatesKernel;
    public RefineCandidates refineCandidatesKernel;
    public CountCandidateRelationships countCandidateRelationshipsKernel;
    public FindCandidateRelationships findCandidateRelationshipsKernel;
    public CountSolutionCombinations countSolutionCombinationsKernel;
    public GenerateSolutionCombinations generateSolutionCombinationsKernel;
    public ValidateSolutions validateSolutionsKernel;
    public CombineRelationships combineRelationshipsKernel;
    public PruneSolutions pruneSolutionsKernel;

    public QueryKernels() throws IOException {
        initializeQueryKernels();
    }

    private void initializeQueryKernels() throws IOException {
        this.context = JavaCL.createBestContext();
        this.queue = this.context.createDefaultQueue();
        this.checkCandidatesKernel = new CheckCandidates(this.context);
        this.exploreCandidatesKernel = new ExploreCandidates(this.context);
        this.refineCandidatesKernel = new RefineCandidates(this.context);
        this.countCandidateRelationshipsKernel = new CountCandidateRelationships(this.context);
        this.findCandidateRelationshipsKernel = new FindCandidateRelationships(this.context);
        this.countSolutionCombinationsKernel = new CountSolutionCombinations(this.context);
        this.generateSolutionCombinationsKernel = new GenerateSolutionCombinations(this.context);
        this.validateSolutionsKernel = new ValidateSolutions(this.context);
        this.combineRelationshipsKernel = new CombineRelationships(this.context);
        this.pruneSolutionsKernel = new PruneSolutions(this.context);
    }
}