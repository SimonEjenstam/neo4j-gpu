package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.*;
import org.neo4j.graphdb.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class GpuQuery {
    private QueryContext queryContext;
    private final QueryKernels queryKernels;
    private BufferContainer bufferContainer;

    public GpuQuery(QueryContext queryContext) throws IOException {
        this.queryContext = queryContext;
        this.queryKernels = new QueryKernels();
        this.bufferContainer = BufferContainerGenerator.generateBufferContainer(this.queryContext, this.queryKernels);
    }

    public CLBuffer<Integer> executeQuery(ArrayList<Node> visitOrder) throws IOException {

        /****** Candidate initialization step ******/
        CandidateInitializer candidateInitializer =
                new CandidateInitializer(this.queryContext, this.queryKernels, this.bufferContainer);
        candidateInitializer.candidateInitialization(visitOrder);

        /****** Candidate refinement step ******/
        CandidateRefinement candidateRefinement =
                new CandidateRefinement(this.queryContext, this.queryKernels, this.bufferContainer);
        candidateRefinement.refine(visitOrder);

        /****** Candidate relationship searching step ******/
        CandidateRelationshipSearcher candidateRelationshipSearcher =
                new CandidateRelationshipSearcher(this.queryContext, this.queryKernels, this.bufferContainer);
        HashMap<Integer, CandidateRelationships> relationshipCandidatesHashMap = candidateRelationshipSearcher.searchCandidateRelationships();

        /****** Candidate relationship joining step ******/
        CandidateRelationshipJoiner candidateRelationshipJoiner =
                new CandidateRelationshipJoiner(this.queryContext, this.queryKernels, this.bufferContainer);
        CLBuffer<Integer> solutions = candidateRelationshipJoiner.joinCandidateRelationships(relationshipCandidatesHashMap);

        return solutions;
    }
}
