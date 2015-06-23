package se.simonevertsson;

import se.simonevertsson.gpu.BufferContainer;
import se.simonevertsson.gpu.QueryContext;
import se.simonevertsson.gpu.QueryKernels;

/**
 * Created by simon on 2015-06-23.
 */
public class MockQuery {

    public final QueryContext queryContext;
    public final QueryKernels queryKernels;
    public final BufferContainer bufferContainer;

    public MockQuery(QueryContext queryContext, QueryKernels queryKernels, BufferContainer bufferContainer) {
        this.queryContext = queryContext;
        this.queryKernels = queryKernels;
        this.bufferContainer = bufferContainer;
    }
}
