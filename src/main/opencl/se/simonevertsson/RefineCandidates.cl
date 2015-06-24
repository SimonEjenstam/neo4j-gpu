bool is_valid_related_node(
    int d_related_node_degree,
    __global int* d_labels, 
    int d_label_index_start,
    int d_label_index_end,

    __global int* q_labels,
    int q_label_index_start,
    int q_label_index_end,  
    int q_related_node_degree) {
    
    
    if(q_labels[q_label_index_start] != -1) {

        for(int i = q_label_index_start; i < q_label_index_end; i++) {      
            
            /* Verify that the related node has the current query label */
            int query_label = q_labels[i];       
            bool label_exists = false;
            for(int m = d_label_index_start; m < d_label_index_end; m++) {
                if(d_labels[m] == query_label) {
                    label_exists = true;
                    break;
                }
            }

            if(!label_exists) {
                /* No match for given query label was found */ 
                return false;
            }
        }

    }

    return q_related_node_degree <= d_related_node_degree;  
}

__kernel void refine_candidates(
    int q_node,
    __global int* q_relationships,
    __global int* q_relationship_types,
    __global int* q_relationship_indices,
    __global int* q_labels,
    __global int* q_label_indices,
    int q_relationship_start_index,
    int q_relationship_end_index,

    __global int* d_relationships,
    __global int* d_relationship_types,
    __global int* d_relationship_indices,
    __global int* d_labels,
    __global int* d_label_indices,

    __global int* candidates_array,
    __global bool* candidate_indicators,
    int d_node_count)
{

    int candidate_node = candidates_array[get_global_id(0)];

    int c_relationship_start_index = d_relationship_indices[candidate_node];
    int c_relationship_end_index = d_relationship_indices[candidate_node+1];

    if(q_relationships[q_relationship_start_index] != -1) {
        /* The query node has relationships */
        for(int i = q_relationship_start_index; i < q_relationship_end_index; i++) {
            int u = q_relationships[i];
            int q_relationship_type = q_relationship_types[i];

            int q_node_degree = q_relationships[ q_relationship_indices[u]] != -1 ? q_relationship_indices[u+1] - q_relationship_indices[u] : 0;
            int q_label_index_start = q_label_indices[u];   
            int q_label_index_end = q_label_indices[u+1];

            bool match_found = false;

            for(int j = c_relationship_start_index; j < c_relationship_end_index; j++) {
                int v = d_relationships[j];
                if(v != -1) {
                    /* The related data node has relationships */
                    if(q_relationships[q_relationship_start_index] != -1) {
                        int d_relationship_type = d_relationship_types[j];
                        if(q_relationship_type == -1 || d_relationship_type == q_relationship_type ) {
                        

                            int d_node_degree = d_relationships[d_relationship_indices[v]] != -1 ? (d_relationship_indices[v+1] - d_relationship_indices[v]) : 0;
                            int d_label_index_start = d_label_indices[v];                      
                            int d_label_index_end = d_label_indices[v+1];

                            if(is_valid_related_node(d_node_degree, d_labels, d_label_index_start, d_label_index_end, q_labels, q_label_index_start, q_label_index_end, q_node_degree)) {
                                match_found = true;
                                break;
                            }
                        }
                    }
                }     
            }

            if(!match_found) {
                candidate_indicators[ q_node*d_node_count + candidate_node ] = false;
                return;
            }
        }
    }
}