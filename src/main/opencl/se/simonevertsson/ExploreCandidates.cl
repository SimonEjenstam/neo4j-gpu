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

__kernel void explore_candidates(
    int q_node,
    __global long* q_adjacencies,
    __global int* q_adjacency_indicies,
    __global int* q_labels,
    __global int* q_label_indicies,
    int q_adjacency_start_index,
    int q_adjacency_end_index,

    __global long* d_adjacencies,
    __global int* d_adjacency_indicies,
    __global int* d_labels,
    __global int* d_label_indicies,

    __global int* candidates_array,
    __global bool* candidate_indicators,
    int d_node_count)
{

    int candidate_node = candidates_array[get_global_id(0)];                //1

    int c_adjacency_start_index = d_adjacency_indicies[candidate_node];     //2
    int c_adjacency_end_index = d_adjacency_indicies[candidate_node+1];     //4

    for(int i = q_adjacency_start_index; i < q_adjacency_end_index; i++) {  //0,1 (0-2)
        int u = q_adjacencies[i];											//2, 3
        int q_node_degree = q_adjacency_indicies[u+1] - q_adjacency_indicies[u];//2 - 0
        int q_label_index_start = q_label_indicies[u];						//2, 3
        int q_label_index_end = q_label_indicies[u+1];						//3, 4

        bool match_found = false;

        for(int j = c_adjacency_start_index; j < c_adjacency_end_index; j++) {  // 2,3 (2-4)
             int v = d_adjacencies[j];											// 2,3
             int degree = d_adjacency_indicies[v+1] - d_adjacency_indicies[v];	// 2,3
             int d_label_index_start = d_label_indicies[v];						
             int d_label_index_end = d_label_indicies[v+1];

             if(is_valid_adjacent_node(v, degree, d_labels, d_label_index_start, d_label_index_end, q_labels, q_label_index_start, q_label_index_end, q_node_degree)) {
                match_found = true;
                break;
             }

        }

        if(!match_found) {
            candidate_indicators[ q_node*d_node_count + candidate_node ] = false;
            return;
        }
    }



    for(int i = q_adjacency_start_index; i < q_adjacency_end_index; i++) {
        int u = q_adjacencies[i];											//2, 3
        int q_node_degree = q_adjacency_indicies[u+1] - q_adjacency_indicies[u];//2 - 0
        int q_label_index_start = q_label_indicies[u];						//2, 3
        int q_label_index_end = q_label_indicies[u+1];						//3, 4

        for(int j = c_adjacency_start_index; j < c_adjacency_end_index; j++) {
             /* Retrieve adjacent node and its degree and label interval */
            int v = d_adjacencies[j];
            int degree = d_adjacency_indicies[v+1] - d_adjacency_indicies[v];
    		int d_label_index_start = d_label_indicies[v];
    		int d_label_index_end = d_label_indicies[v+1];

            candidate_indicators[ u*d_node_count + v ] = 
             	is_valid_adjacent_node(v, degree, d_labels, d_label_index_start, d_label_index_end, q_labels, q_label_index_start, q_label_index_end, q_node_degree);

             /*
             int d_label_index_start = d_label_indicies[v];
             int d_label_index_end = d_label_indicies[v];
             int labels_match = 1;
             for(int k = q_label_index_start; k < q_label_index_end; k++) {
                 int query_label = q_labels[k];
                 bool label_exists = false;
                 for(int m = d_label_index_start; m < d_label_index_end; m++) {
                     if(d_labels[m] == query_label) {
                         label_exists = true;
                         break;
                     }
                 }
                 if(!label_exists) {
                     labels_match = 0;
                     break;
                 }
             }
             candidate_indicators[ u*d_node_count + v ] = (labels_match == 1  && q_node_degree <= (d_adjacency_indicies[v+1] - d_adjacency_indicies[v])) ;
             */

        }
    }
}