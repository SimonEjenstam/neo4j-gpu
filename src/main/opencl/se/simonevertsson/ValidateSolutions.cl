__kernel void validate_solutions(
    int q_start_node,
    int q_end_node,
    int query_node_count,
    __global int* possible_solutions,

    __global int* c_start_nodes,
    __global int* c_end_node_indices,
    __global int* c_end_nodes,

    int start_node_count,

    __global bool* validation_indicators
    )
{
    int possible_solution_index = get_global_id(0);

    for(int i = 0; i < start_node_count; i++) {
        if(c_start_nodes[i] == possible_solutions[possible_solution_index*query_node_count + q_start_node]) {
            int end_node_index_start = c_end_node_indices[i];
            int end_node_index_end = c_end_node_indices[i+1];
            for(int j = end_node_index_start; j < end_node_index_end; j++) {

                if(c_end_nodes[j] == possible_solutions[possible_solution_index*query_node_count + q_end_node]) {
                    validation_indicators[possible_solution_index] = true;
                    return;
                }

            }
        }
    }
}