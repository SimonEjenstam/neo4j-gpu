package se.simonevertsson.gpu.buffer;

public class BufferContainer {
    public DataBuffers dataBuffers;
    public QueryBuffers queryBuffers;

    public BufferContainer(DataBuffers dataBuffers, QueryBuffers queryBuffers) {
        this.dataBuffers = dataBuffers;
        this.queryBuffers = queryBuffers;
    }
}