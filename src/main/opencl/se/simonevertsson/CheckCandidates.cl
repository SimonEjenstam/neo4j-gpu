__kernel void check_candidates(__global int* adjacency_indicies, int query_vertex_degree, __global int* labels, __global int* label_indicies, __global int* query_vertex_labels, int query_vertex_label_count, __global int* c_set, int n)
{
    int i = get_global_id(0);
    int start_label_index = label_indicies[i];
    int end_label_index = label_indicies[i+1];
    int labels_match = 1;
    for(int j = 0; j < query_vertex_label_count; j++) {
        int query_label = query_vertex_labels[j];
        bool label_exists = false;
        for(int k = start_label_index; k < end_label_index; k++) {
            if(labels[k] == query_label) {
                label_exists = true;
                break;
            }
        }
        if(!label_exists) {
            labels_match = 0;
            break;
        }
    }

    c_set[i] = (labels_match == 1  && query_vertex_degree <= (adjacency_indicies[i+1] - adjacency_indicies[i])) ;
}