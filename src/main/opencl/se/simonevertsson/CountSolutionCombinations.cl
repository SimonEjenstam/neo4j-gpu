__kernel void count_solution_combinations(
    int q_start_node,
    int q_end_node,
    __global int* possible_solutions,

    __global int* c_start_nodes,
    __global int* c_end_node_indices,
    __global int* c_end_nodes,

    __global bool* start_node_visited,
    int start_node_count,

    __global int* combination_counts
    )
{
    int possible_solution_index = get_global_id(0);
    combination_counts[possible_solution_index] = 0;

    /* The START node of the query edge is the visited node */
    if(start_node_visited[0]) {
        for(int i = 0; i < start_node_count; i++) {
            if(c_start_nodes[i] == possible_solutions[possible_solution_index + q_start_node]) {
                combination_counts[possible_solution_index] = c_end_node_indices[i+1];
                return;
            }
        }
    }

     /* The END node of the query edge is the visited node */
    int count = 0;
    for(int i = 0; i < start_node_count; i++) {
        int end_node_index_start = c_end_node_indices[i];
        int end_node_index_end = c_end_node_indices[i+1];
        for(int j = end_node_index_start; j < end_node_index_end; j++) {
            count += (c_end_nodes[j] == possible_solutions[possible_solution_index + q_end_node]);
        }
    }
    combination_counts[possible_solution_index] = count;
}