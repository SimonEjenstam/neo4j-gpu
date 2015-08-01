__kernel void count_solution_combinations(
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

    __global bool* start_node_visited,
    int start_node_count,

    __global int* combination_counts
    )
{
    int possible_solution_index = get_global_id(0);
    combination_counts[possible_solution_index] = 0;
    int count = 0;

    /* The START node of the query edge is the visited node */
    if(start_node_visited[0]) {     
        for(int i = 0; i < start_node_count; i++) {
            if(c_start_nodes[i] == possible_solution_elements[possible_solution_index*query_node_count + q_start_node]) {
                int end_node_index_start = c_end_node_indices[i];
                int end_node_index_end = c_end_node_indices[i+1];
                for(int j = end_node_index_start; j < end_node_index_end; j++) {
                    int relationship_index = c_relationship_indices[j];
                    bool relationship_valid = true;
                    for(int k = 0; k < query_relationship_count; k++) {
                        if(possible_solution_relationships[possible_solution_index*query_relationship_count + k] == relationship_index) {
                            relationship_valid = false;
                            break;
                        }
                    }
                    count += (relationship_valid ? 1 : 0);
                }
            }
        }
    } else{
        /* The END node of the query edge is the visited node */
        for(int i = 0; i < start_node_count; i++) {
            int end_node_index_start = c_end_node_indices[i];
            int end_node_index_end = c_end_node_indices[i+1];
            for(int j = end_node_index_start; j < end_node_index_end; j++) {
                //count += (c_end_nodes[j] == possible_solution_elements[possible_solution_index*query_node_count + q_end_node]) ? 1 : 0;
                if(c_end_nodes[j] == possible_solution_elements[possible_solution_index*query_node_count + q_end_node]) {
                    int relationship_index = c_relationship_indices[j];
                    bool relationship_valid = true;
                    for(int k = 0; k < query_relationship_count; k++) {
                        if(possible_solution_relationships[possible_solution_index*query_relationship_count + k] == relationship_index) {
                            relationship_valid = false;
                            break;
                        }        
                    }
                    count += (relationship_valid ? 1 : 0);
                }             
            }
        }
    }
     
    combination_counts[possible_solution_index] = count;
}