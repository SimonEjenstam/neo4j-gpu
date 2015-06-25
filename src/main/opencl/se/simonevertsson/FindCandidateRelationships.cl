__kernel void find_candidate_relationships(
    int q_start_node,
    int q_end_node,
    __global int* d_relationships,
    __global int* d_relationship_indices,

    __global int* candidate_relationship_end_nodes,
    __global int* candidate_relationship_end_node_indices,

    __global int* candidate_relationship_start_nodes,
    __global bool* candidate_indicators,
    int d_node_count)
{
    int workset_index = get_global_id(0);
    
    int candidate_start_node = candidate_relationship_start_nodes[workset_index];

    int c_relationship_start_index = d_relationship_indices[candidate_start_node];
    int c_relationship_end_index = d_relationship_indices[candidate_start_node+1];

    int current_output_index = candidate_relationship_end_node_indices[workset_index];
    for(int i = c_relationship_start_index; i < c_relationship_end_index; i++) {

        int candidate_end_node = d_relationships[i];
        if(candidate_indicators[q_end_node*d_node_count + candidate_end_node]) {
            candidate_relationship_end_nodes[current_output_index] = candidate_end_node;
            current_output_index++;
        }
    }
}