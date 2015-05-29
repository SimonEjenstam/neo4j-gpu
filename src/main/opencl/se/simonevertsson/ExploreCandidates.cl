__kernel void explore_candidates(int query_vertex,  int query_vertex_degree, __global int* query_vertex_adjacency __global int* labels, __global int* label_indicies, __global int* query_vertex_labels, int query_vertex_label_count,
                                __global int* c_array, __global int* adjacency_indicies, __global int* c_set, int n)
{
    int candidate_vertex = c_array[get_global_id(0];

    int candidate_vertex_adjacency_start_index =
    int query_vertex_adjacency_start_index = adjacency_indicies[query_vertex];
    int query_vertex_adjacency_end_index = adjacency_indicies[query_vertex+1];

    for(int i = query_vertex_start_index; i < query_vertex_adjacency_end_index; i++) {

    }

}