#pragma once

#include <cassert>
#include <limits>
#include <vector>

/**
 * Ward hierarchical clustering with optimal leaf ordering
 *
 * Ward criterion: merge clusters that minimize increase in total within-cluster variance
 *   Delta = (n_i * n_j) / (n_i + n_j) * ||c_i - c_j||^2
 *
 * Optimal leaf ordering: minimize sum of adjacent leaf distances while preserving
 * the hierarchical structure. Uses greedy optimization at each internal node.
 *
 * @param data  D×N matrix (data[d][n] = dimension d, sample n)
 * @return      N integers: optimal leaf order permutation for coloring
 */
inline std::vector<int> ward_leaf_order(const std::vector<std::vector<float>> &data) {
  assert(!data.empty());
  const size_t D = data.size();
  const size_t N = data[0].size();
  assert(N > 0);

  if (N == 1)
    return {0};

  // Squared Euclidean distance between two points
  auto point_dist_sq = [&](size_t i, size_t j) -> float {
    float dist_sq = 0.0f;
    for (size_t d = 0; d < D; ++d) {
      float diff = data[d][i] - data[d][j];
      dist_sq += diff * diff;
    }
    return dist_sq;
  };

  // Cluster info
  struct Cluster {
    std::vector<float> centroid;
    size_t size;
    int left, right; // children (-1 for leaves)
  };

  // Initialize: N leaf clusters + space for N-1 internal nodes
  std::vector<Cluster> clusters(2 * N - 1);
  for (size_t n = 0; n < N; ++n) {
    clusters[n].centroid.resize(D);
    for (size_t d = 0; d < D; ++d) {
      clusters[n].centroid[d] = data[d][n];
    }
    clusters[n].size = 1;
    clusters[n].left = clusters[n].right = -1;
  }

  // Ward distance between two clusters
  auto ward_dist = [&](size_t i, size_t j) -> float {
    float dist_sq = 0.0f;
    for (size_t d = 0; d < D; ++d) {
      float diff = clusters[i].centroid[d] - clusters[j].centroid[d];
      dist_sq += diff * diff;
    }
    float n_i = static_cast<float>(clusters[i].size);
    float n_j = static_cast<float>(clusters[j].size);
    return (n_i * n_j) / (n_i + n_j) * dist_sq;
  };

  // Active cluster indices
  std::vector<size_t> active;
  active.reserve(N);
  for (size_t n = 0; n < N; ++n) {
    active.push_back(n);
  }

  // Merge N-1 times
  size_t next_cluster = N;
  while (active.size() > 1) {
    // Find best pair to merge
    float best_dist = std::numeric_limits<float>::max();
    size_t best_i = 0, best_j = 1;

    for (size_t ii = 0; ii < active.size(); ++ii) {
      for (size_t jj = ii + 1; jj < active.size(); ++jj) {
        float d = ward_dist(active[ii], active[jj]);
        if (d < best_dist) {
          best_dist = d;
          best_i = ii;
          best_j = jj;
        }
      }
    }

    size_t ci = active[best_i];
    size_t cj = active[best_j];

    // Create new merged cluster
    Cluster &merged = clusters[next_cluster];
    merged.centroid.resize(D);
    float n_i = static_cast<float>(clusters[ci].size);
    float n_j = static_cast<float>(clusters[cj].size);
    float n_total = n_i + n_j;
    for (size_t d = 0; d < D; ++d) {
      merged.centroid[d] =
          (n_i * clusters[ci].centroid[d] + n_j * clusters[cj].centroid[d]) / n_total;
    }
    merged.size = clusters[ci].size + clusters[cj].size;
    merged.left = static_cast<int>(ci);
    merged.right = static_cast<int>(cj);

    // Update active list
    active.erase(active.begin() + best_j);
    active.erase(active.begin() + best_i);
    active.push_back(next_cluster);

    ++next_cluster;
  }

  // Optimal leaf ordering using dynamic programming
  // For each subtree, track: leftmost leaf, rightmost leaf, ordered leaves
  struct SubtreeInfo {
    size_t left_leaf;  // leftmost leaf in optimal ordering
    size_t right_leaf; // rightmost leaf in optimal ordering
    std::vector<int> order;
  };

  std::vector<SubtreeInfo> info(2 * N - 1);

  // Process bottom-up (leaves first, then internal nodes in merge order)
  // Leaves
  for (size_t n = 0; n < N; ++n) {
    info[n].left_leaf = n;
    info[n].right_leaf = n;
    info[n].order = {static_cast<int>(n)};
  }

  // Internal nodes (processed in order of creation = merge order)
  for (size_t node = N; node < 2 * N - 1; ++node) {
    size_t L = static_cast<size_t>(clusters[node].left);
    size_t R = static_cast<size_t>(clusters[node].right);

    // Four possible orderings based on which ends are adjacent:
    // 1. L_order + R_order: L.right adjacent to R.left
    // 2. L_order + R_order_reversed: L.right adjacent to R.right
    // 3. L_order_reversed + R_order: L.left adjacent to R.left
    // 4. L_order_reversed + R_order_reversed: L.left adjacent to R.right

    float dist_LR_RL = point_dist_sq(info[L].right_leaf, info[R].left_leaf);
    float dist_LR_RR = point_dist_sq(info[L].right_leaf, info[R].right_leaf);
    float dist_LL_RL = point_dist_sq(info[L].left_leaf, info[R].left_leaf);
    float dist_LL_RR = point_dist_sq(info[L].left_leaf, info[R].right_leaf);

    // Find minimum
    float min_dist = dist_LR_RL;
    int best_config = 0;
    if (dist_LR_RR < min_dist) { min_dist = dist_LR_RR; best_config = 1; }
    if (dist_LL_RL < min_dist) { min_dist = dist_LL_RL; best_config = 2; }
    if (dist_LL_RR < min_dist) { min_dist = dist_LL_RR; best_config = 3; }

    // Build order based on best config
    auto &L_order = info[L].order;
    auto &R_order = info[R].order;
    auto &result = info[node].order;
    result.reserve(L_order.size() + R_order.size());

    switch (best_config) {
    case 0: // L + R
      result.insert(result.end(), L_order.begin(), L_order.end());
      result.insert(result.end(), R_order.begin(), R_order.end());
      info[node].left_leaf = info[L].left_leaf;
      info[node].right_leaf = info[R].right_leaf;
      break;
    case 1: // L + R_reversed
      result.insert(result.end(), L_order.begin(), L_order.end());
      result.insert(result.end(), R_order.rbegin(), R_order.rend());
      info[node].left_leaf = info[L].left_leaf;
      info[node].right_leaf = info[R].left_leaf;
      break;
    case 2: // L_reversed + R
      result.insert(result.end(), L_order.rbegin(), L_order.rend());
      result.insert(result.end(), R_order.begin(), R_order.end());
      info[node].left_leaf = info[L].right_leaf;
      info[node].right_leaf = info[R].right_leaf;
      break;
    case 3: // L_reversed + R_reversed
      result.insert(result.end(), L_order.rbegin(), L_order.rend());
      result.insert(result.end(), R_order.rbegin(), R_order.rend());
      info[node].left_leaf = info[L].right_leaf;
      info[node].right_leaf = info[R].left_leaf;
      break;
    }
  }

  // Root is the last cluster created
  return info[2 * N - 2].order;
}
