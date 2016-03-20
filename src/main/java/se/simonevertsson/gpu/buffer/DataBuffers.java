package se.simonevertsson.gpu.buffer;

import com.nativelibs4java.opencl.CLBuffer;

public class DataBuffers {
  public CLBuffer<Integer> dataRelationshipIndicesBuffer;
  public CLBuffer<Integer> dataLabelsBuffer;
  public CLBuffer<Integer> dataLabelIndicesBuffer;
  public CLBuffer<Integer> dataNodeRelationshipsBuffer;
  public CLBuffer<Integer> dataRelationshipTypesBuffer;
}