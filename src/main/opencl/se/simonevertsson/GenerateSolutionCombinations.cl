__kernel void generate_solution_combinations(
    int q_start_node,
    int q_end_node,
    int q_relationship,
    int query_node_count,
    int query_relationship_count,
    __global int* old_possible_solution_elements,
    __global int* old_possible_solution_relationships,
    __global int* combination_indices,

    __global int* c_start_nodes,
    __global int* c_end_node_indices,
    __global int* c_end_nodes,
    __global int* c_relationship_indices,

    __global bool* start_node_visited,
    int start_node_count,

    __global int* possible_solution_elements,
    __global int* possible_solution_relationships
    )
{
    int possible_solution_index = get_global_id(0);
    int output_start_index = combination_indices[possible_solution_index];
    int output_end_index = combination_indices[possible_solution_index+1];
    int offset = 0;

    /* The START node of the query edge is the visited node */
    if(start_node_visited[0]) {
        for(int i = 0; i < start_node_count; i++) {
            if(c_start_nodes[i] == old_possible_solution_elements[possible_solution_index*query_node_count + q_start_node]) {
                int end_node_index_start = c_end_node_indices[i];
                int end_node_index_end = c_end_node_indices[i+1];
                for(int j = end_node_index_start; j < end_node_index_end; j++) {

                    int relationship_index = c_relationship_indices[j];
                    bool relationship_valid = true;
                    for(int k = 0; k < query_relationship_count; k++) {
                        if(old_possible_solution_relationships[possible_solution_index*query_relationship_count + k] == relationship_index) {
                            relationship_valid = false;
                            break;
                        }
                    }

                    if(relationship_valid) {
                        /* Calculate new indices */
                        int old_combo_index = possible_solution_index*query_node_count;
                        int old_relationship_index = possible_solution_index*query_relationship_count;

                        int new_combo_index = (output_start_index+offset)*query_node_count;
                        int new_relationship_index = (output_start_index+offset)*query_relationship_count;

                        /* Copy old combination */
                        for(int l = 0; l < query_node_count; l++ ) {
                            possible_solution_elements[new_combo_index + l] = old_possible_solution_elements[old_combo_index + l];
                        }

                        /* Copy old relationships */
                        for(int m = 0; m < query_relationship_count; m++ ) {
                            possible_solution_relationships[new_relationship_index + m] = 
                                old_possible_solution_relationships[old_relationship_index + m];
                        }

                        /* Add end node to solution */
                        possible_solution_elements[new_combo_index + q_end_node] = c_end_nodes[j];

                        /* Add relationship to solution */
                        possible_solution_relationships[new_relationship_index + q_relationship] = relationship_index;

                        /* Increase offset and determine if we are done */
                        offset++;
                        if(offset >= output_end_index) {
                            return;
                        }
                    }
                }

            }
        }
    } else {
        /* The END node of the query edge is the visited node */
        for(int i = 0; i < start_node_count; i++) {
            int end_node_index_start = c_end_node_indices[i];
            int end_node_index_end = c_end_node_indices[i+1];
            for(int j = end_node_index_start; j < end_node_index_end; j++) {

                if(c_end_nodes[j] == old_possible_solution_elements[possible_solution_index*query_node_count + q_end_node]) {

                    int relationship_index = c_relationship_indices[j];
                    bool relationship_valid = true;
                    for(int k = 0; k < query_relationship_count; k++) {
                        if(old_possible_solution_relationships[possible_solution_index*query_relationship_count + k] == relationship_index) {
                            relationship_valid = false;
                            break;
                        }
                    }

                    if(relationship_valid) {
                        /* Calculate new indices */
                        int old_combo_index = possible_solution_index*query_node_count;
                        int old_relationship_index = possible_solution_index*query_relationship_count;

                        int new_combo_index = (output_start_index+offset)*query_node_count;
                        int new_relationship_index = (output_start_index+offset)*query_relationship_count;

                        /* Copy old combination */
                        for(int l = 0; l < query_node_count; l++ ) {
                            possible_solution_elements[new_combo_index + l] = old_possible_solution_elements[old_combo_index + l];
                        }

                        /* Copy old relationships */
                        for(int m = 0; m < query_relationship_count; m++ ) {
                            possible_solution_relationships[new_relationship_index + m] = 
                                old_possible_solution_relationships[old_relationship_index + m];
                        }

                        /* Add start node to solution */
                        possible_solution_elements[new_combo_index + q_start_node] = c_start_nodes[i];

                        /* Add relationship to solution */
                        possible_solution_relationships[new_relationship_index + q_relationship] = relationship_index;

                        /* Increase offset and determine if we are done */
                        offset++;
                        if(offset >= output_end_index) {
                            return;
                        }
                    }
                }
                
            }
        }
    }    
}