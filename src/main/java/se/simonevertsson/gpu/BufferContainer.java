package se.simonevertsson.gpu;

public class BufferContainer {
    DataBuffers dataBuffers;
    QueryBuffers queryBuffers;

    public BufferContainer(DataBuffers dataBuffers, QueryBuffers queryBuffers) {
        this.dataBuffers = dataBuffers;
        this.queryBuffers = queryBuffers;
    }
}