#ifndef CLIENT_HPP_
#define CLIENT_HPP_

#include <chrono>
#include <fsatutils/parameter/parameter.hpp>
#include <fsatutils/zmq/parameter_protocol.hpp>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace fsatutils {

namespace zmq {

class Client {
 public:
  struct CommandArg {
    std::string name;
    std::string value;
  };

  struct CommandRequest {
    std::string name;
    std::vector<CommandArg> args;
  };

  Client(std::string host);
  ~Client();

  bool sendCommand(std::string_view service, Client::CommandRequest& req);
  bool sendDiscover();
  bool recvAndLogResponses();
  bool publishRawBytes(std::string_view topic, std::span<std::uint8_t> data);

  bool subscribeParameter(std::string_view service, std::string_view name);
  bool unsubscribeParameter(std::string_view service, std::string_view name);
  bool requestParameter(std::string_view service, std::string_view name);
  bool setParameter(std::string_view service, std::string_view name,
                    ParameterSystem::Value const& value);
  std::optional<ParameterUpdate> receiveParameter(
      std::chrono::milliseconds timeout);

 private:
  class impl;
  std::unique_ptr<impl> impl_;
};

}  // namespace zmq

}  // namespace fsatutils

#endif
