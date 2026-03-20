#ifndef ZMQ_ENGINE_HPP_
#define ZMQ_ENGINE_HPP_

#include <cstdint>
#include <span>
#include <string_view>

namespace fsatutils {

namespace zmq {

class ZMQEngine {
 public:
  ZMQEngine();
  ZMQEngine(std::string& host, std::size_t xpub, std::size_t xsub);
  ~ZMQEngine();

  int publish_raw_bytes(std::string_view topic, std::span<uint8_t> data) const;

  int subscribe_to(std::string_view topic) const;
  int unsubscribe(std::string_view topic) const;
  int configure_zprotocol(std::string& service_name) const;

  auto ctx() const { return ctx_; };
  auto sub() const { return sub_; };
  auto pub() const { return pub_; };

 private:
  void* ctx_;
  void* sub_;
  void* pub_;
};

}  // namespace zmq

}  // namespace fsatutils

#endif  // ZMQ_ENGINE_H_
