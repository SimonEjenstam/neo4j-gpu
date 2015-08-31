__kernel void validate_solutions(
    int q_start_node,
    int q_end_node,
    int query_node_count,
    int query_relationship_count,
    __global int* possible_solution_elements,
    __global int* possible_solution_relationships,

    __global int* c_start_nodes,
    __global int* c_end_node_indices,
    __global int* c_end_nodes,
    __global int* c_relationship_indices,

    int start_node_count,

    __global bool* valid_relationships_indicators
    )
{
    int possible_solution_index = get_global_id(0);
    
    valid_relationships_indicators[possible_solution_index] = false;

    for(int i = 0; i < start_node_count; i++) {
        if(c_start_nodes[i] == possible_solution_elements[possible_solution_index*query_node_count + q_start_node]) {
            int end_node_index_start = c_end_node_indices[i];
            int end_node_index_end = c_end_node_indices[i+1];
            for(int j = end_node_index_start; j < end_node_index_end; j++) {

                if(c_end_nodes[j] == possible_solution_elements[possible_solution_index*query_node_count + q_end_node]) {
                    int relationship_index = c_relationship_indices[j];
                    bool relationship_valid = true;
                    for(int k = 0; k < query_relationship_count; k++) {
                        if(possible_solution_relationships[possible_solution_index*query_relationship_count + k] == relationship_index) {
                            relationship_valid = false;
                            break;
                        }
                    }
                    if(relationship_valid) {
                        valid_relationships_indicators[possible_solution_index] = true;
                        return;
                    }
                }

            }
        }
    }
}