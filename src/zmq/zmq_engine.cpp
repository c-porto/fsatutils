#include <zmq.h>

#include <fsatutils/errors.hpp>
#include <fsatutils/zmq/zmq_engine.hpp>
#include <fsatutils/zmq/zprotocol.hpp>

namespace fsatutils {

namespace zmq {

ZMQEngine::ZMQEngine() {
  ctx_ = zmq_ctx_new();

  if (ctx_ == nullptr) {
    logs::log(ERR, "Failed to create zmq context!\n");
    throw_runtime_error("Failed to create ZMQ context");
  }

  pub_ = zmq_socket(ctx_, ZMQ_PUB);

  if (pub_ == nullptr) {
    logs::log(ERR, "Failed to create zmq publisher!\n");
    zmq_ctx_destroy(ctx_);
    throw_runtime_error("Failed to create ZMQ publisher");
  }

  sub_ = zmq_socket(ctx_, ZMQ_SUB);

  if (sub_ == nullptr) {
    logs::log(ERR, "Failed to create zmq subscriber!\n");
    zmq_close(pub_);
    zmq_ctx_destroy(ctx_);
    throw_runtime_error("Failed to create ZMQ subscriber");
  }

  const char* xsub = "tcp://0.0.0.0:2808";

  if (zmq_connect(pub_, xsub) != 0) {
    logs::log(ERR, "Failed to connect to engine xsub!\n");
    zmq_close(pub_);
    zmq_close(sub_);
    zmq_ctx_destroy(ctx_);
    throw_runtime_error("Failed to connect to engine xsub");
  }

  const char* xpub = "tcp://0.0.0.0:2809";

  if (zmq_connect(sub_, xpub) != 0) {
    logs::log(ERR, "Failed to connect to engine xpub!\n");
    zmq_close(pub_);
    zmq_close(sub_);
    zmq_ctx_destroy(ctx_);
    throw_runtime_error("Failed to connect to engine xpub");
  }

  logs::log(INFO,
            "Connected to ZMQ Engine: pub(tx): [2808], sub(rx): [2809]\n");
}

ZMQEngine::ZMQEngine(std::string& host, std::size_t xpub, std::size_t xsub) {
  ctx_ = zmq_ctx_new();

  if (ctx_ == nullptr) {
    logs::log(ERR, "Failed to create zmq context!\n");
    throw_runtime_error("Failed to create ZMQ context");
  }

  pub_ = zmq_socket(ctx_, ZMQ_PUB);

  if (pub_ == nullptr) {
    logs::log(ERR, "Failed to create zmq publisher!\n");
    zmq_ctx_destroy(ctx_);
    throw_runtime_error("Failed to create ZMQ publisher");
  }

  sub_ = zmq_socket(ctx_, ZMQ_SUB);

  if (sub_ == nullptr) {
    logs::log(ERR, "Failed to create zmq subscriber!\n");
    zmq_close(pub_);
    zmq_ctx_destroy(ctx_);
    throw_runtime_error("Failed to create ZMQ subscriber");
  }

  std::string xs = "tcp://" + host + ":" + std::to_string(xsub);

  if (zmq_connect(pub_, xs.c_str()) != 0) {
    logs::log(ERR, "Failed to connect to engine xsub!\n");
    zmq_close(pub_);
    zmq_close(sub_);
    zmq_ctx_destroy(ctx_);
    throw_runtime_error("Failed to connect to engine xsub");
  }

  std::string xp = "tcp://" + host + ":" + std::to_string(xpub);

  if (zmq_connect(sub_, xp.c_str()) != 0) {
    logs::log(ERR, "Failed to connect to engine xpub!\n");
    zmq_close(pub_);
    zmq_close(sub_);
    zmq_ctx_destroy(ctx_);
    throw_runtime_error("Failed to connect to engine xpub");
  }

  logs::log(INFO,
            "Connected to ZMQ Engine[%llu]: pub(tx): [%llu], sub(rx): [%s]\n",
            host.c_str(), xsub, xpub);
}

ZMQEngine::~ZMQEngine() {
  if (zmq_ctx_shutdown(ctx_) < 0) {
    logs::log(ERR, "Failed to shutdown ZMQ context");
  }

  if (zmq_close(sub_) < 0) {
    logs::log(ERR, "Failed to close ZMQ subscriber");
  }

  if (zmq_close(pub_) < 0) {
    logs::log(ERR, "Failed to close ZMQ publisher");
  }

  if (zmq_ctx_destroy(ctx_) < 0) {
    logs::log(ERR, "Failed to destroy ZMQ context");
  }
}

int ZMQEngine::publish_raw_bytes(std::string_view topic,
                                 std::span<uint8_t> data) const {
  if (zmq_send(pub_, topic.data(), topic.size(), ZMQ_SNDMORE) < 0) {
    logs::log(ERR, "Failed to send topic! ZMQ error [%s]\n",
              zmq_strerror(errno));
    return -1;
  }

  if (zmq_send(pub_, data.data(), data.size(), 0) < 0) {
    logs::log(ERR, "Failed to send data! ZMQ error [%s]\n",
              zmq_strerror(errno));
    return -1;
  }

  return 0;
}

int ZMQEngine::subscribe_to(std::string_view topic) const {
  std::string topic_name{topic};

  if (zmq_setsockopt(sub_, ZMQ_SUBSCRIBE, topic.data(), topic.size()) != 0) {
    logs::log(ERR, "Failed to subscribe to %s!\n", topic_name.c_str());
    return -1;
  }

  logs::log(INFO, "Subscribed to %s\n", topic_name.c_str());

  return 0;
}

int ZMQEngine::unsubscribe(std::string_view topic) const {
  std::string topic_name{topic};

  if (zmq_setsockopt(sub_, ZMQ_UNSUBSCRIBE, topic.data(), topic.size()) != 0) {
    logs::log(ERR, "Failed to unsubscribe to %s!\n", topic_name.c_str());
    return -1;
  }

  logs::log(INFO, "Unsubscribed to %s\n", topic_name.c_str());

  return 0;
}

int ZMQEngine::configure_zprotocol(std::string& service_name) const {
  if (zmq_setsockopt(sub_, ZMQ_SUBSCRIBE, service_name.c_str(),
                     service_name.size()) != 0) {
    logs::log(ERR, "Failed to subscribe to service name!\n");
    return -1;
  }

  if (zmq_setsockopt(sub_, ZMQ_SUBSCRIBE, g_discoverTopic.data(), 4U) != 0) {
    logs::log(ERR, "Failed to subscribe to discover command!\n");
    return -1;
  }

  logs::log(INFO, "Applied filters: %s; %s\n", service_name.c_str(),
            g_discoverTopic.data());

  return 0;
}

}  // namespace zmq

}  // namespace fsatutils
