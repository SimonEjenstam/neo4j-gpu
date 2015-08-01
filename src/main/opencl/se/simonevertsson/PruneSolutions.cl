__kernel void prune_solutions(
    int q_node_count,
    int q_relationship_count,
    int q_relationhip,

    __global int* old_possible_solution_elements,
    __global int* old_possible_solution_relationships,
    __global int* valid_relationships,
    __global int* output_indices,

    __global int* pruned_possible_solution_elements,
    __global int* pruned_possible_solution_relationships
    )
{
    int possible_solution_index = get_global_id(0);

    if(valid_relationships[possible_solution_index] != -1) {
        int old_solution_element_index = (possible_solution_index)*q_node_count;
        int old_solution_relationship_index = (possible_solution_index)*q_relationship_count;
        int new_solution_element_index = (output_indices[possible_solution_index])*q_node_count;
        int new_solution_relationship_index = (output_indices[possible_solution_index])*q_relationship_count;

        /* Copy solution elements to new indices */
        for(int i = 0; i < q_node_count; i++ ) {
            pruned_possible_solution_elements[new_solution_element_index + i] = 
                old_possible_solution_elements[old_solution_element_index + i];
        }

        /* Copy solution relationships to new indices */
        for(int j = 0; j < q_relationship_count; j++ ) {
            pruned_possible_solution_relationships[new_solution_relationship_index + j] = 
                old_possible_solution_relationships[old_solution_relationship_index + j];
        }

        /* Add current valid relationship to relationship indices */
        pruned_possible_solution_relationships[new_solution_relationship_index + q_relationhip] = valid_relationships[possible_solution_index];
    }
}