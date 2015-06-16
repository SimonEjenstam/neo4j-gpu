__kernel void prune_solutions(
    int q_node_count,
    __global int* old_possible_solutions,
    __global bool* validation_indicators,
    __global int* output_indices,

    __global int* pruned_possible_solutions
    )
{
    int possible_solution_index = get_global_id(0);

    if(validation_indicators[possible_solution_index]) {
        int old_solution_index = (possible_solution_index)*q_node_count;
        int new_solution_index = (output_indices[possible_solution_index])*q_node_count;
        for(int i = 0; i < q_node_count; i++ ) {
            pruned_possible_solutions[new_solution_index + i] = old_possible_solutions[old_solution_index + i];
        }
    }
}