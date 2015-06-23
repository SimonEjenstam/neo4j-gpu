__kernel void check_candidates(
    __global int* q_node_labels,
    int q_node,
    int q_node_label_start_index,
    int q_node_label_end_index,
    int q_node_degree,

    __global int* d_label_indices,
    __global int* d_labels,
    __global int* d_relationships,
    __global int* d_relationship_indices,

    __global bool* candidate_indicators,
    int data_node_count)
{
    int i = get_global_id(0);

    int d_start_label_index = d_label_indices[i];
    int d_end_label_index = d_label_indices[i+1];
    bool labels_match = true;

    if(q_node_labels[q_node_label_start_index] != -1) {
        for(int j = q_node_label_start_index; j < q_node_label_end_index; j++) {
            int q_node_label = q_node_labels[j];
            bool label_exists = false;
            for(int k = d_start_label_index; k < d_end_label_index; k++) {
                if(d_labels[k] == q_node_label) {
                    label_exists = true;
                    break;
                }
            }
            if(!label_exists) {
                labels_match = false;
                break;
            }
        }
    }

    int d_node_degree = d_relationships[d_relationship_indices[i]] == -1 ? 0 : (d_relationship_indices[i+1] - d_relationship_indices[i]);

    candidate_indicators[ q_node*data_node_count + i ] = (labels_match  && q_node_degree <= d_node_degree);
}