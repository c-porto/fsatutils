#include <zmq.h>

#include <array>
#include <fsatutils/errors.hpp>
#include <fsatutils/log/log.hpp>
#include <fsatutils/zmq/client.hpp>
#include <fsatutils/zmq/zmq_engine.hpp>
#include <fsatutils/zmq/zprotocol.hpp>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <thread>

using json = nlohmann::json;

namespace fsatutils {

namespace zmq {

class Client::impl {
 public:
  impl(std::string host);

  bool sendCommand(std::string_view service, Client::CommandRequest& req);

  bool sendDiscover();

  bool recvAndLogResponses();

  bool publishRawBytes(std::string_view topic, std::span<std::uint8_t> data);

 private:
  ZMQEngine engine_;
  std::string host_;
};

Client::Client(std::string host) : impl_{std::make_unique<impl>(host)} {}

Client::~Client() = default;

bool Client::sendCommand(std::string_view service,
                         Client::CommandRequest& req) {
  return impl_->sendCommand(service, req);
}

bool Client::sendDiscover() { return impl_->sendDiscover(); }

bool Client::recvAndLogResponses() { return impl_->recvAndLogResponses(); }

bool Client::publishRawBytes(std::string_view topic,
                             std::span<std::uint8_t> data) {
  return impl_->publishRawBytes(topic, data);
}

Client::impl::impl(std::string host)
    : engine_{host, ZMQ_FLATSAT_ENGINE_XPUB_PORT, ZMQ_FLATSAT_ENGINE_XSUB_PORT},
      host_{host} {

  using namespace std::chrono_literals;

  /* Make sure subscribers can be registered */
  std::this_thread::sleep_for(100ms);

  if (zmq_setsockopt(engine_.sub(), ZMQ_SUBSCRIBE, "", 0U) != 0) {
    logs::log(ERR, "Failed to subscribe to every topic!\n");
    throw_runtime_error("Failed to subscribe to every topic!");
  }
  if (zmq_setsockopt(engine_.sub(), ZMQ_UNSUBSCRIBE, "disc", 4U) != 0) {
    logs::log(ERR, "Failed to unsubscribe to discover topic!\n");
    throw_runtime_error("Failed to unsubscribe to discover topic!");
  }
}

bool Client::impl::sendCommand(std::string_view service,
                               Client::CommandRequest& req) {
  json command;

  command["command"] = req.name;

  json args = json::array();

  for (auto const& arg : req.args) {
    json a;

    a["name"] = arg.name;
    a["value"] = arg.value;

    args.push_back(a);
  }

  command["args"] = args;

  std::string payload = command.dump();

  CommandMsgHeader header = {.version = 1, .proto = MessageProtocol::JSON};

  if (zmq_send(engine_.pub(), service.data(), service.size(), ZMQ_SNDMORE) <
      0) {
    logs::log(ERR, "Failed to send service name! ZMQ error [%s]\n",
              zmq_strerror(errno));
    return false;
  }

  std::array<const std::uint8_t, 2> buf = {
      header.version, static_cast<std::uint8_t>(header.proto)};

  if (zmq_send(engine_.pub(), buf.data(), buf.size(), ZMQ_SNDMORE) < 0) {
    logs::log(ERR, "Failed to send command header! ZMQ error [%s]\n",
              zmq_strerror(errno));
    return false;
  }

  if (zmq_send(engine_.pub(), payload.data(), payload.size(), 0) < 0) {
    logs::log(ERR, "Failed to send JSON payload! ZMQ error [%s]\n",
              zmq_strerror(errno));
    return false;
  }

  return true;
}

bool Client::impl::sendDiscover() {
  DiscoverMsgHeader header = {.version = 1};

  if (zmq_send(engine_.pub(), g_discoverTopic.data(), g_discoverTopic.size(),
               ZMQ_SNDMORE) < 0) {
    logs::log(ERR, "Failed to send discover topic! ZMQ error [%s]\n",
              zmq_strerror(errno));
    return false;
  }

  std::array<const std::uint8_t, 1> buf = {header.version};

  if (zmq_send(engine_.pub(), buf.data(), buf.size(), 0) < 0) {
    logs::log(ERR, "Failed to send discover header! ZMQ error [%s]\n",
              zmq_strerror(errno));
    return false;
  }

  return true;
}

bool Client::impl::recvAndLogResponses() {
  using clock = std::chrono::steady_clock;
  using namespace std::chrono_literals;
  constexpr std::chrono::milliseconds window{800ms};

  std::array<char, ZMQ_FLATSAT_ENGINE_MTU> buf;

  bool received_any = false;

  const auto deadline = clock::now() + window;

  zmq_pollitem_t item = {
      .socket = engine_.sub(), .fd = 0, .events = ZMQ_POLLIN, .revents = 0};

  while (1) {
    auto now = clock::now();

    if (now >= deadline) return received_any;

    auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);

    int rc = zmq_poll(&item, 1, remaining.count());

    if (rc == -1) {
      logs::log(ERR, "ZMQ Poll failed [%s]", zmq_strerror(errno));
      return false;
    }

    if (rc == 0) {
      return received_any;
    }

    if (item.revents & ZMQ_POLLIN) {
      while (1) {
        int events = 0;
        size_t size = sizeof(events);

        zmq_getsockopt(engine_.sub(), ZMQ_EVENTS, &events, &size);

        if (!(events & ZMQ_POLLIN)) break;

        int res = zmq_recv(engine_.sub(), buf.data(), buf.size(), 0);

        if (res < 0) {
          logs::log(ERR, "Failed to receve message! ZMQ error [%s]",
                    zmq_strerror(errno));
          return false;
        }

        received_any = true;

        std::string_view payload{buf.data(), static_cast<std::size_t>(res)};

        if (payload.empty()) continue;
        if (payload.starts_with("disc")) continue;

        try {
          json j = json::parse(payload);
          std::cout << "Discovered: " << j.dump(1) << std::endl;
        } catch (const std::exception&) {
          logs::log(ERR, "Failed to parse JSON string for discover message!");
        }
      }
    }
  }
}

bool Client::impl::publishRawBytes(std::string_view topic,
                                   std::span<std::uint8_t> data) {
  return (engine_.publish_raw_bytes(topic, data) == 0) ? true : false;
}

}  // namespace zmq

}  // namespace fsatutils
