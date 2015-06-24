__kernel void count_candidate_relationships(
    int q_start_node,
    int q_end_node,
    __global int* d_relationships,
    __global int* d_relationship_indices,

    __global int* candidate_relationship_counts,
    __global int* candidates_array,
    __global bool* candidate_indicators,
    int d_node_count)
{
    int workset_index = get_global_id(0);
    candidate_relationship_counts[workset_index] = 0;

    int candidate_start_node = candidates_array[workset_index];

    int c_relationship_start_index = d_relationship_indices[candidate_start_node];
    int c_relationship_end_index = d_relationship_indices[candidate_start_node+1];

    for(int i = c_relationship_start_index; i < c_relationship_end_index; i++) {
        int candidate_end_node = d_relationships[i];
        candidate_relationship_counts[workset_index] += candidate_indicators[q_end_node*d_node_count + candidate_end_node];
    }
}