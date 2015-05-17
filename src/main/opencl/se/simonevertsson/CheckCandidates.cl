__kernel void check_candidates(__global const int* labels, int query_vertex_label, __global int* c_set, int n)
{
    int i = get_global_id(0);
    c_set[i] = (labels[i] == query_vertex_label);
}