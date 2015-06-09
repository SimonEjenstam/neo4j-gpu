bool is_valid_adjacent_node(
    int adjacent_node,
    int adjacent_node_degree,
    __global int* d_labels, 
    int d_label_index_start,
    int d_label_index_end,

    __global int* q_labels,
    int q_label_index_start,
    int q_label_index_end,  
    int q_degree) {
    
    

    for(int i = q_label_index_start; i < q_label_index_end; i++) {    	
        
        /* Verify that the adjacent node has the current query label */
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

    return q_degree <= adjacent_node_degree;	
}

__kernel void refine_candidates(
    int q_node,
    __global int* q_adjacencies,
    __global int* q_adjacency_indicies,
    __global int* q_labels,
    __global int* q_label_indicies,
    int q_adjacency_start_index,
    int q_adjacency_end_index,

    __global int* d_adjacencies,
    __global int* d_adjacency_indicies,
    __global int* d_labels,
    __global int* d_label_indicies,

    __global int* candidates_array,
    __global bool* candidate_indicators,
    int d_node_count)
{

    int candidate_node = candidates_array[get_global_id(0)];

    int c_adjacency_start_index = d_adjacency_indicies[candidate_node];
    int c_adjacency_end_index = d_adjacency_indicies[candidate_node+1];

    for(int i = q_adjacency_start_index; i < q_adjacency_end_index; i++) {
        int u = q_adjacencies[i];
        int q_node_degree = q_adjacency_indicies[u+1] - q_adjacency_indicies[u];
        int q_label_index_start = q_label_indicies[u];	
        int q_label_index_end = q_label_indicies[u+1];

        bool match_found = false;

        for(int j = c_adjacency_start_index; j < c_adjacency_end_index; j++) {
             int v = d_adjacencies[j];
             int degree = d_adjacency_indicies[v+1] - d_adjacency_indicies[v];
             int d_label_index_start = d_label_indicies[v];						
             int d_label_index_end = d_label_indicies[v+1];

             if(is_valid_adjacent_node(v, degree, d_labels, d_label_index_start, d_label_index_end, q_labels, q_label_index_start, q_label_index_end, q_node_degree)) {
                match_found = true;
                break;
             }

        }

        if(!match_found) {
            candidate_indicators[ q_node*d_node_count + candidate_node ] = false;
        }
    }
}