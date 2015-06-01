__kernel void check_candidates(
    __global int* q_node_labels,
    int q_node,
    int q_node_label_start_index,
    int q_node_label_end_index,
    int q_node_degree,

    __global int* d_label_indicies,
    __global int* d_labels,
    __global int* d_adjacency_indicies,

    __global bool* candidate_indicators,
    int data_node_count)
{
    int i = get_global_id(0);
    int d_start_label_index = d_label_indicies[i];
    int d_end_label_index = d_label_indicies[i+1];
    int labels_match = 1;
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
            labels_match = 0;
            break;
        }
    }

    candidate_indicators[ q_node*data_node_count + i ] = (labels_match == 1  && q_node_degree <= (d_adjacency_indicies[i+1] - d_adjacency_indicies[i])) ;
}