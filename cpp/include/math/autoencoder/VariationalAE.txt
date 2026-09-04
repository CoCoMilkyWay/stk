#pragma once

#include <torch/nn/functional.h>
#include <torch/nn/module.h>
#include <torch/optim.h>
#include <torch/torch.h>

#include <model/define/CBuffer.hpp>
#include <model/define/DataType.hpp>
#include <model/misc/print.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <stdexcept>

namespace model::math::autoencoder {
// NOTE: input data should be already normalized to [0,1] range

/**
 * Variational Autoencoder (VAE) for dimension reduction and feature mining
 *
 * Parameters provided by upper layer at compile time:
 * - InputDim: Input dimension from upper layer
 * - LatentDim: Latent dimension from upper layer
 * - HiddenDim: Hidden layer dimension from upper layer
 * - BatchSize: Batch size from upper layer
 * - Epochs: Number of epochs from upper layer
 *
 * Features:
 * - Simple implementation of parameters from upper layer
 * - Compile-time static constants
 * - Incremental training on replay buffers
 * - Dimension reduction for feature mining
 * - Fixed-size compile-time optimized data structures
 * - Assumes input data is already normalized to [0,1] range

 */

inline constexpr int VAE_LAYERS_NUM = 2;
inline constexpr int VAE_INPUT_DIM = NUM_PIPS;
inline constexpr int VAE_HIDDEN_DIM = 24;
inline constexpr int VAE_LATENT_DIM = 3;
inline constexpr int VAE_BATCH_SIZE = 512;
inline constexpr int VAE_EPOCHS = 100;
inline constexpr float VAE_EPS = 1e-8f;
inline constexpr float VAE_LEARNING_RATE = 5e-4f;
inline constexpr bool VAE_USE_BATCH_NORM = false;
inline constexpr float VAE_BATCH_NORM_MOMENTUM = 0.9f;
inline constexpr bool VAE_USE_DROPOUT = false;
inline constexpr float VAE_DROPOUT_RATE = 0.2f;
inline constexpr int VAE_NUM_ENCODE_SAMPLES = 8; // for prediction, multiple samples to capture uncertainty (in reparameterization trick)
inline constexpr bool PRINT_TRAINING_PROGRESS = true;
inline constexpr int NUM_TRAINING_SAMPLES = 60 * 24 * 5 * 4 * 3 / NUM_BARS_PER_PIP; // quarterly(subsampled)
static_assert(REPLAY_BUFER_DEPTH >= NUM_TRAINING_SAMPLES, "REPLAY_BUFER_DEPTH must be greater than NUM_TRAINING_SAMPLES");

template <int NumLayers, int InputDim, int LatentDim, int HiddenDim, int BatchSize, int Epochs>
class VAE : public torch::nn::Module {
  // =========================================================================
  // COMPILE-TIME VALIDATION
  // =========================================================================
  static_assert(NumLayers > 0, "NumLayers must be positive");
  static_assert(InputDim > 0, "InputDim must be positive");
  static_assert(LatentDim > 0, "LatentDim must be positive");
  static_assert(HiddenDim > 0, "HiddenDim must be positive");
  static_assert(BatchSize > 0, "BatchSize must be positive");
  static_assert(Epochs > 0, "Epochs must be positive");

private:
  // =========================================================================
  // NETWORK ARCHITECTURE
  // =========================================================================
  torch::nn::Sequential encoder_{nullptr};
  torch::nn::Linear encoder_mu{nullptr};
  torch::nn::Linear encoder_logvar{nullptr};
  torch::nn::Sequential decoder_{nullptr};

  // =========================================================================
  // HYPERPARAMETERS (COMPILE-TIME CONSTANTS)
  // =========================================================================
  static constexpr int hidden_dim_ = HiddenDim;
  static constexpr float learning_rate_ = VAE_LEARNING_RATE;
  static constexpr int batch_size_ = BatchSize;
  static constexpr int epochs_ = Epochs;

  static constexpr float eps_ = VAE_EPS;
  static constexpr bool use_batch_norm_ = VAE_USE_BATCH_NORM;
  static constexpr float batch_norm_momentum_ = VAE_BATCH_NORM_MOMENTUM;
  static constexpr bool use_dropout_ = VAE_USE_DROPOUT;
  static constexpr float dropout_rate_ = VAE_DROPOUT_RATE;
  static constexpr int num_encode_samples_ = VAE_NUM_ENCODE_SAMPLES;

  // =========================================================================
  // TRAINING STATE
  // =========================================================================
  std::unique_ptr<torch::optim::Adam> optimizer_;
  bool is_trained_;

  // =========================================================================
  // TRAINING METRICS
  // =========================================================================
  struct TrainingMetrics {
    std::array<float, Epochs> reconstruction_losses;
    std::array<float, Epochs> kl_losses;
    std::array<float, Epochs> total_losses;
    std::chrono::system_clock::time_point last_training_time;
    size_t total_samples_trained{0};
    size_t current_epoch{0};

    void reset() {
      reconstruction_losses.fill(0.0f);
      kl_losses.fill(0.0f);
      total_losses.fill(0.0f);
      total_samples_trained = 0;
      current_epoch = 0;
    }

    void add_loss(float recon_loss, float kl_loss, float total_loss) {
      if (current_epoch < Epochs) {
        reconstruction_losses[current_epoch] = recon_loss;
        kl_losses[current_epoch] = kl_loss;
        total_losses[current_epoch] = total_loss;
        ++current_epoch;
      }
    }

    std::pair<const float *, size_t> get_reconstruction_losses() const { return {reconstruction_losses.data(), current_epoch}; }
    std::pair<const float *, size_t> get_kl_losses() const { return {kl_losses.data(), current_epoch}; }
    std::pair<const float *, size_t> get_total_losses() const { return {total_losses.data(), current_epoch}; }
  };
  TrainingMetrics metrics_;

  // =========================================================================
  // HIGH-FREQUENCY ENCODING BUFFER
  // =========================================================================
  mutable torch::Tensor input_buffer_;

  // =========================================================================
  // INITIALIZATION HELPERS
  // =========================================================================
  void initialize_encoder_layers() {
    // Create Sequential encoder with integrated activations (matching Python architecture)
    auto layers = torch::nn::Sequential();

    // Build encoder layers: Linear -> BatchNorm -> ReLU -> (Dropout)
    for (int i = 0; i < NumLayers; ++i) {
      int input_size = (i == 0) ? InputDim : hidden_dim_;
      layers->push_back(torch::nn::Linear(input_size, hidden_dim_));

      if constexpr (use_batch_norm_) {
        auto bn_options = torch::nn::BatchNorm1dOptions(hidden_dim_).momentum(batch_norm_momentum_);
        layers->push_back(torch::nn::BatchNorm1d(bn_options));
      }

      layers->push_back(torch::nn::ReLU());

      if constexpr (use_dropout_) {
        layers->push_back(torch::nn::Dropout(torch::nn::DropoutOptions(dropout_rate_)));
      }
    }

    encoder_ = register_module("encoder", layers);

    // VAE output layers
    encoder_mu = register_module("encoder_mu", torch::nn::Linear(hidden_dim_, LatentDim));
    encoder_logvar = register_module("encoder_logvar", torch::nn::Linear(hidden_dim_, LatentDim));
  }

  void initialize_decoder_layers() {
    // Create Sequential decoder with integrated activations (matching Python architecture)
    auto layers = torch::nn::Sequential();

    // Hidden layers (symmetric to encoder)
    for (int i = 0; i < NumLayers; ++i) {
      int input_size = (i == 0) ? LatentDim : hidden_dim_;
      layers->push_back(torch::nn::Linear(input_size, hidden_dim_));
      layers->push_back(torch::nn::ReLU());
    }

    // Final output layer with integrated sigmoid
    layers->push_back(torch::nn::Linear(hidden_dim_, InputDim));
    layers->push_back(torch::nn::Sigmoid());

    decoder_ = register_module("decoder", layers);
  }

  void initialize_weights() {
    // Initialize all Linear layers in encoder
    for (auto &module : encoder_->modules(false)) {
      if (auto linear = std::dynamic_pointer_cast<torch::nn::LinearImpl>(module)) {
        torch::nn::init::xavier_uniform_(linear->weight);
        torch::nn::init::zeros_(linear->bias);
      }
    }

    // Initialize VAE output layers
    torch::nn::init::xavier_uniform_(encoder_mu->weight);
    torch::nn::init::zeros_(encoder_mu->bias);
    torch::nn::init::xavier_uniform_(encoder_logvar->weight);
    torch::nn::init::zeros_(encoder_logvar->bias);

    // Initialize all Linear layers in decoder
    for (auto &module : decoder_->modules(false)) {
      if (auto linear = std::dynamic_pointer_cast<torch::nn::LinearImpl>(module)) {
        torch::nn::init::xavier_uniform_(linear->weight);
        torch::nn::init::zeros_(linear->bias);
      }
    }
  }

  void initialize_network() {
    initialize_encoder_layers();
    initialize_decoder_layers();
    initialize_weights();
  }

  void setup_optimizer() { optimizer_ = std::make_unique<torch::optim::Adam>(this->parameters(), torch::optim::AdamOptions(learning_rate_)); }

  void initialize_input_buffer() {
    // Create persistent 2D tensor that owns its memory (not a view)
    input_buffer_ = torch::zeros({1, static_cast<long>(InputDim)}, torch::kFloat32);
  }

  void reset_training_state() {
    is_trained_ = false;
    metrics_.reset();
  }

  // Consolidated initialization method used by constructor, copy constructor, and reset
  void common_initialization() {
    initialize_network();
    setup_optimizer();
    initialize_input_buffer();
    reset_training_state();
    this->train();
  }

  // =========================================================================
  // WEIGHT COPYING (FOR COPY CONSTRUCTOR)
  // =========================================================================
  void copy_weights_from(const VAE &other) {
    // Copy all modules in encoder (Linear, BatchNorm, etc.)
    auto this_encoder_modules = encoder_->modules(false);
    auto other_encoder_modules = other.encoder_->modules(false);

    auto it_this = this_encoder_modules.begin();
    auto it_other = other_encoder_modules.begin();

    while (it_this != this_encoder_modules.end() && it_other != other_encoder_modules.end()) {
      // Copy Linear layers
      if (auto linear_this = std::dynamic_pointer_cast<torch::nn::LinearImpl>(*it_this)) {
        if (auto linear_other = std::dynamic_pointer_cast<torch::nn::LinearImpl>(*it_other)) {
          linear_this->weight.data().copy_(linear_other->weight.data());
          linear_this->bias.data().copy_(linear_other->bias.data());
        }
      }
      // Copy BatchNorm layers
      else if (auto bn_this = std::dynamic_pointer_cast<torch::nn::BatchNorm1dImpl>(*it_this)) {
        if (auto bn_other = std::dynamic_pointer_cast<torch::nn::BatchNorm1dImpl>(*it_other)) {
          bn_this->weight.data().copy_(bn_other->weight.data());
          bn_this->bias.data().copy_(bn_other->bias.data());
          bn_this->running_mean.data().copy_(bn_other->running_mean.data());
          bn_this->running_var.data().copy_(bn_other->running_var.data());
        }
      }
      ++it_this;
      ++it_other;
    }

    // Copy VAE output layers
    encoder_mu->weight.data().copy_(other.encoder_mu->weight.data());
    encoder_mu->bias.data().copy_(other.encoder_mu->bias.data());
    encoder_logvar->weight.data().copy_(other.encoder_logvar->weight.data());
    encoder_logvar->bias.data().copy_(other.encoder_logvar->bias.data());

    // Copy all modules in decoder
    auto this_decoder_modules = decoder_->modules(false);
    auto other_decoder_modules = other.decoder_->modules(false);

    auto it_dec_this = this_decoder_modules.begin();
    auto it_dec_other = other_decoder_modules.begin();

    while (it_dec_this != this_decoder_modules.end() && it_dec_other != other_decoder_modules.end()) {
      if (auto linear_this = std::dynamic_pointer_cast<torch::nn::LinearImpl>(*it_dec_this)) {
        if (auto linear_other = std::dynamic_pointer_cast<torch::nn::LinearImpl>(*it_dec_other)) {
          linear_this->weight.data().copy_(linear_other->weight.data());
          linear_this->bias.data().copy_(linear_other->bias.data());
        }
      }
      ++it_dec_this;
      ++it_dec_other;
    }
  }

  // =========================================================================
  // DATA PROCESSING HELPERS
  // =========================================================================
  template <size_t BufferSize>
  torch::Tensor buffer_to_tensor(const CBuffer<std::array<float, InputDim>, BufferSize> &buffer) const {
    size_t buffer_size = buffer.size();
    if (buffer_size == 0) [[unlikely]] {
      return torch::empty({0, static_cast<long>(InputDim)}, torch::kFloat32);
    }

    auto buffer_span = buffer.span();
    torch::Tensor tensor = torch::zeros({static_cast<long>(buffer_size), static_cast<long>(InputDim)}, torch::kFloat32);
    float *tensor_data = tensor.data_ptr<float>();

    size_t idx = 0;
    // Process head span
    for (const auto &sample : buffer_span.head) {
      std::memcpy(tensor_data + idx * InputDim, sample.data(), sizeof(float) * InputDim);
      ++idx;
    }
    // Process tail span
    for (const auto &sample : buffer_span.tail) {
      std::memcpy(tensor_data + idx * InputDim, sample.data(), sizeof(float) * InputDim);
      ++idx;
    }

    return tensor;
  }

public:
  // =========================================================================
  // CONSTRUCTORS AND DESTRUCTOR
  // =========================================================================
  explicit VAE() { common_initialization(); }

  VAE(const VAE &other) : is_trained_(other.is_trained_), metrics_(other.metrics_) {
    common_initialization();
    copy_weights_from(other);

    this->eval(); // Copied model should be in eval mode
  }

  VAE &operator=(const VAE &) = delete;
  ~VAE() = default;

  // =========================================================================
  // CORE VAE OPERATIONS
  // =========================================================================
  std::pair<torch::Tensor, torch::Tensor> encode(const torch::Tensor &x) {
    // Sequential encoder handles all layers and activations
    torch::Tensor h = encoder_->forward(x);

    torch::Tensor mu = encoder_mu->forward(h);
    torch::Tensor logvar = encoder_logvar->forward(h);
    return {mu, logvar};
  }

  // For training/forward: single sample per input
  torch::Tensor reparameterize(const torch::Tensor &mu, const torch::Tensor &logvar) const {
    torch::Tensor std = torch::exp(0.5 * logvar); // σ = exp(log_var/2)
    torch::Tensor eps = torch::randn_like(std);   // ε ~ N(0,1)
    return mu + eps * std;                        // z = μ + σ * ε
  }

  // For prediction/encode: n_samples per input (vectorized)
  torch::Tensor reparameterize_samples(const torch::Tensor &mu, const torch::Tensor &logvar, int n_samples) const {
    // mu, logvar: [batch_size, latent_dim] or [1, latent_dim]
    auto batch_size = mu.size(0);
    auto latent_dim = mu.size(1);
    // Expand mu and logvar to [n_samples, batch_size, latent_dim]
    torch::Tensor mu_exp = mu.unsqueeze(0).expand({n_samples, batch_size, latent_dim});
    torch::Tensor logvar_exp = logvar.unsqueeze(0).expand({n_samples, batch_size, latent_dim});
    torch::Tensor std = torch::exp(0.5 * logvar_exp);
    torch::Tensor eps = torch::randn({n_samples, batch_size, latent_dim}, mu.options());
    return mu_exp + eps * std;
  }

  torch::Tensor decode(const torch::Tensor &z) {
    return decoder_->forward(z); // Sequential handles all layers and activations including sigmoid
  }

  std::tuple<torch::Tensor, torch::Tensor, torch::Tensor, torch::Tensor> forward(const torch::Tensor &x) {
    auto [mu, logvar] = encode(x);
    torch::Tensor z = reparameterize(mu, logvar); // batch-only for training
    torch::Tensor recon = decode(z);
    return {recon, mu, logvar, z};
  }

  // =========================================================================
  // LOSS COMPUTATION
  // =========================================================================
  std::tuple<torch::Tensor, torch::Tensor, torch::Tensor> compute_loss(const torch::Tensor &recon, const torch::Tensor &x, const torch::Tensor &mu, const torch::Tensor &logvar, int epoch) const {

    float batch_size = static_cast<float>(x.size(0));

    // Reconstruction loss (MSE)
    torch::Tensor recon_loss = torch::mse_loss(recon, x, torch::Reduction::Mean);

    // KL divergence loss
    torch::Tensor kl_loss = -0.5 * torch::mean(torch::sum(1 + logvar - mu.pow(2) - logvar.exp(), 1));

    // Loss weighting and trade-offs:
    // NOTE: less reconstruction loss -> behave like normal autoencoder -> cluster maybe further apart
    // NOTE: less kl_div = closer to uni-gaussian = less channel capacity = more disentangled = features(latent_dim) are uncorrelated
    // recon weight (α-annealing) promote disentanglement at start
    float alpha = 1.0f;
    // KL weight    (β-annealing) promote reconstruction at start
    float beta = std::min(1.0f, static_cast<float>(epoch) / 10.0f);

    torch::Tensor total_loss = alpha * recon_loss + beta * kl_loss;
    return {total_loss, recon_loss, kl_loss};
  }

  // =========================================================================
  // TRAINING METHODS
  // =========================================================================
  template <size_t BufferSize>
  bool train_on_buffer(const CBuffer<std::array<float, InputDim>, BufferSize> &buffer, bool verbose = false) {
    if (buffer.empty()) [[unlikely]]
      return false;

    torch::Tensor data = buffer_to_tensor(buffer);
    return train_on_tensor(data, verbose);
  }

  bool train_on_tensor(const torch::Tensor &data, bool verbose = false) {
    if (data.size(0) == 0) [[unlikely]]
      return false;

    // Ensure we have enough samples for at least one full batch
    assert(data.size(0) >= batch_size_ && "Data must contain at least batch_size samples for training");

    torch::Tensor training_data = data.clone();
    metrics_.reset();
    metrics_.last_training_time = std::chrono::system_clock::now();

    this->train();

    float best_loss = std::numeric_limits<float>::max();
    int patience = is_trained_ ? 4 : 6;
    int epochs_no_improve = 0;

    // Setup random number generator for shuffling
    std::random_device rd;
    std::mt19937 gen(rd());

    for (int epoch = 0; epoch < epochs_; ++epoch) {
      float epoch_total_loss = 0.0f;
      float epoch_recon_loss = 0.0f;
      float epoch_kl_loss = 0.0f;
      int num_batches = 0;

      // Create indices for shuffling
      std::vector<int64_t> indices(training_data.size(0));
      std::iota(indices.begin(), indices.end(), 0);
      std::shuffle(indices.begin(), indices.end(), gen);

      // Process data in batches
      for (size_t i = 0; i < indices.size(); i += batch_size_) {
        size_t end_idx = std::min(i + batch_size_, indices.size());
        size_t current_batch_size = end_idx - i;
        if (current_batch_size < 2) [[unlikely]] {
          continue;
        }
        // Efficient batch selection using index_select
        std::vector<int64_t> batch_indices_vec(indices.begin() + i, indices.begin() + end_idx);
        auto batch_indices = torch::from_blob(batch_indices_vec.data(), {static_cast<long>(batch_indices_vec.size())}, torch::kLong).clone();
        torch::Tensor batch_data = training_data.index_select(0, batch_indices);

        auto [recon, mu, logvar, z] = forward(batch_data);
        auto [total_loss, recon_loss, kl_loss] = compute_loss(recon, batch_data, mu, logvar, epoch);

        optimizer_->zero_grad();
        total_loss.backward();
        optimizer_->step();

        epoch_total_loss += total_loss.template item<float>();
        epoch_recon_loss += recon_loss.template item<float>();
        epoch_kl_loss += kl_loss.template item<float>();
        ++num_batches;
        metrics_.total_samples_trained += batch_data.size(0);
      }
      if (num_batches > 0) {
        float avg_total_loss = epoch_total_loss / num_batches;
        float avg_recon_loss = epoch_recon_loss / num_batches;
        float avg_kl_loss = epoch_kl_loss / num_batches;
        metrics_.add_loss(avg_recon_loss, avg_kl_loss, avg_total_loss);
        if (avg_total_loss < best_loss - 1e-3f) {
          best_loss = avg_total_loss;
          epochs_no_improve = 0;
        } else {
          epochs_no_improve++;
        }
        if (epochs_no_improve >= patience) {
          break;
        }
      } else {
        metrics_.add_loss(0.0f, 0.0f, 0.0f);
      }
      if (verbose && (epoch % 1 == 0 || epoch == epochs_ - 1)) {
        auto [total_losses, size] = metrics_.get_total_losses();
        auto [recon_losses, recon_size] = metrics_.get_reconstruction_losses();
        auto [kl_losses, kl_size] = metrics_.get_kl_losses();
        if (size > 0) {
          size_t idx = size - 1;
          idx = std::min({idx, recon_size > 0 ? recon_size - 1 : 0, kl_size > 0 ? kl_size - 1 : 0});
          std::cout << std::left << "Epoch " << std::setw(4) << (epoch + 1) << "/" << std::setw(4) << epochs_ << " | "
                    << "L_Tot: " << std::fixed << std::setprecision(5) << std::setw(12) << total_losses[idx] << " | "
                    << "L_Rec: " << std::fixed << std::setprecision(5) << std::setw(12) << recon_losses[idx] << " | "
                    << "L_KL: " << std::fixed << std::setprecision(5) << std::setw(12) << kl_losses[idx] << std::endl;
        }
      }
    }
    is_trained_ = true;
    this->eval();
    return true;
  }

  // =========================================================================
  // ENCODING METHODS
  // =========================================================================
  std::array<float, LatentDim> encode_single(const std::array<float, InputDim> &sample) {
    if (!is_trained_) [[unlikely]] {
      throw std::runtime_error("VAE must be trained before encoding");
    }
    this->eval();
    torch::NoGradGuard no_grad;
    std::memcpy(input_buffer_.data_ptr<float>(), sample.data(), sizeof(float) * InputDim);
    auto [mu, logvar] = encode(input_buffer_); // [1, latent_dim]
    constexpr int n_samples = num_encode_samples_;
    // Vectorized reparameterization for prediction
    torch::Tensor z_samples = reparameterize_samples(mu, logvar, n_samples); // [n_samples, 1, latent_dim]
    torch::Tensor z_mean = z_samples.mean(0);                                // [1, latent_dim]
    auto z_accessor = z_mean.accessor<float, 2>();
    std::array<float, LatentDim> result{};
    for (size_t i = 0; i < LatentDim; ++i) {
      result[i] = z_accessor[0][i];
    }
    return result;
  }

  std::vector<std::array<float, LatentDim>> encode_batch(const std::vector<std::array<float, InputDim>> &samples) {
    if (!is_trained_) [[unlikely]] {
      throw std::runtime_error("VAE must be trained before encoding");
    }
    if (samples.empty()) [[unlikely]]
      return {};
    this->eval();
    torch::NoGradGuard no_grad;
    torch::Tensor input = torch::from_blob(const_cast<float *>(reinterpret_cast<const float *>(samples.data())), {static_cast<int64_t>(samples.size()), static_cast<int64_t>(InputDim)}, torch::kFloat32).clone();
    auto [mu, logvar] = encode(input); // [batch_size, latent_dim]
    if (mu.size(1) != LatentDim) [[unlikely]] {
      throw std::runtime_error("Encoder output dimension mismatch");
    }
    constexpr int n_samples = num_encode_samples_;
    torch::Tensor z_samples = reparameterize_samples(mu, logvar, n_samples); // [n_samples, batch_size, latent_dim]
    torch::Tensor z_mean = z_samples.mean(0);                                // [batch_size, latent_dim]
    auto z_accessor = z_mean.accessor<float, 2>();
    std::vector<std::array<float, LatentDim>> results(samples.size());
    for (size_t i = 0; i < samples.size(); ++i) {
      for (size_t j = 0; j < LatentDim; ++j) {
        results[i][j] = z_accessor[i][j];
      }
    }
    return results;
  }

  // =========================================================================
  // UTILITY METHODS
  // =========================================================================
  const TrainingMetrics &get_metrics() const { return metrics_; }
  bool can_encode() const { return is_trained_; }

  void reset() {
    initialize_weights(); // re-init parameters
    setup_optimizer();    // fresh optimiser state
    initialize_input_buffer();
    reset_training_state();
    this->train();
  }
};

// Explicit template instantiation declarations for common cases
extern template class VAE<2, 6, 3, 24, 256, 100>; // For PipPatternMiner

} // namespace model::math::autoencoder