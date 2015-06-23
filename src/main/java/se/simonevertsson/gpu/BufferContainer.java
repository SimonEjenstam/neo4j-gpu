package se.simonevertsson.gpu;

public class BufferContainer {
    DataBuffers dataBuffers;
    public QueryBuffers queryBuffers;

    public BufferContainer(DataBuffers dataBuffers, QueryBuffers queryBuffers) {
        this.dataBuffers = dataBuffers;
        this.queryBuffers = queryBuffers;
    }
}