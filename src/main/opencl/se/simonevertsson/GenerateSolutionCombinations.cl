__kernel void generate_solution_combinations(
    int q_start_node,
    int q_end_node,
    int q_node_count,
    __global int* old_possible_solutions,
    __global int* combination_indices,

    __global int* c_start_nodes,
    __global int* c_end_node_indices,
    __global int* c_end_nodes,

    __global bool* start_node_visited,
    int start_node_count,

    __global int* possible_solutions
    )
{
    int possible_solution_index = get_global_id(0);
    int output_start_index = combination_indices[possible_solution_index];
    int output_end_index = combination_indices[possible_solution_index+1];
    int offset = 0;

    /* The START node of the query edge is the visited node */
    if(start_node_visited[0]) {
        for(int i = 0; i < start_node_count; i++) {
            if(c_start_nodes[i] == old_possible_solutions[possible_solution_index + q_start_node]) {
                int end_node_index_start = c_end_node_indices[i];
                int end_node_index_end = c_end_node_indices[i+1];
                for(int j = end_node_index_start; j < end_node_index_end; j++) {

                    /* Copy old combination */
                    int old_combo_index = (possible_solution_index+offset)*q_node_count;
                    int new_combo_index = (output_start_index+offset)*q_node_count;
                    for(int k = 0; k < q_node_count; k++ ) {
                        possible_solutions[new_combo_index + k] = old_possible_solutions[old_combo_index + k];
                    }

                    /* Add end node to solution */
                    possible_solutions[new_combo_index + q_end_node] = c_end_nodes[j];

                    /* Increase offset and determine if we are done */
                    offset++;
                    if(offset >= output_end_index) {
                        return;
                    }
                }

            }
        }
    }

     /* The END node of the query edge is the visited node */

    for(int i = 0; i < start_node_count; i++) {
        int end_node_index_start = c_end_node_indices[i];
        int end_node_index_end = c_end_node_indices[i+1];
        for(int j = end_node_index_start; j < end_node_index_end; j++) {

            if(c_end_nodes[j] == old_possible_solutions[possible_solution_index + q_end_node]) {

                /* Copy old combination */
                int old_combo_index = (possible_solution_index+offset)*q_node_count;
                int new_combo_index = (output_start_index+offset)*q_node_count;
                for(int k = 0; k < q_node_count; k++ ) {
                    possible_solutions[new_combo_index + k] = old_possible_solutions[old_combo_index + k];
                }

                /* Add end node to solution */
                possible_solutions[new_combo_index + q_start_node] = c_start_nodes[i];

                /* Increase offset and determine if we are done */
                offset++;
                if(offset >= output_end_index) {
                    return;
                }
            }
            
        }
    }
}