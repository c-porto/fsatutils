#include <zmq.h>

#include <array>
#include <cassert>
#include <cstring>
#include <fsatutils/errors.hpp>
#include <fsatutils/log/log.hpp>
#include <fsatutils/zmq/service.hpp>
#include <fsatutils/zmq/zmq_engine.hpp>
#include <fsatutils/zmq/zprotocol.hpp>
#include <fstream>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>

namespace fsatutils {

namespace zmq {

class Service::impl {
  struct RegistryData {
    std::vector<CommandArg> args;
    std::vector<std::pair<CommandHandlerFn, void*>> handlers;
  };

 public:
  impl(ServiceDescription desc);

  void runService();
  void stopService();

  void cleanResources();

  bool registerCommand(CommandType command, std::vector<CommandArg> args,
                       std::optional<CommandHandlerFn> handler,
                       std::optional<void*> handlerData);

  bool registerHandler(CommandType& command, Service::CommandHandlerFn handler,
                       void* handlerData);

  void workTask(std::stop_token token);

  bool subscribeTo(std::string_view topic);

  bool publishRawBytes(std::string_view topic, std::span<std::uint8_t> data);

 private:
  std::variant<std::monostate, Command, DiscoverMsgHeader, std::string>
  parseMessage(std::span<std::uint8_t, ZMQ_FLATSAT_ENGINE_MTU> buf,
               std::span<uint8_t> topic, int more, std::size_t more_size);

  bool runCommandHandler(Command cmd);

  std::vector<char> serializeServiceDescription();

  bool connectToEngineProxy();

  ServiceDescription desc_;
  ZMQEngine engine_;
  std::unordered_map<CommandType, RegistryData> command_registry_;
  std::jthread work_thread_;
};

Service::Service(ServiceDescription desc)
    : impl_{std::make_unique<Service::impl>(desc)} {}

Service::~Service() { impl_->cleanResources(); }

void Service::runService() { impl_->runService(); }

void Service::stopService() { impl_->stopService(); }

Service& Service::registerCommand(CommandType command,
                                  std::vector<CommandArg> args,
                                  std::optional<CommandHandlerFn> handler,
                                  std::optional<void*> handlerData) {
  if (!impl_->registerCommand(command, args, handler, handlerData)) {
    logs::log(ERR, "Failed to register command [%s]\n", command.c_str());
  }

  return *this;
}

bool Service::registerHandler(CommandType& command,
                              Service::CommandHandlerFn handler,
                              void* handlerData) {
  return impl_->registerHandler(command, handler, handlerData);
}

bool Service::subscribeTo(std::string_view topic) {
  return impl_->subscribeTo(topic);
}

bool Service::publishRawBytes(std::string_view topic,
                              std::span<std::uint8_t> data) {
  return impl_->publishRawBytes(topic, data);
}

Service::impl::impl(ServiceDescription desc) : desc_{std::move(desc)} {
  if (!connectToEngineProxy()) {
    throw_runtime_error("Failed to connect to FlatSat2 ZMQ Engine!");
  }
}

void Service::impl::runService() {
  std::ofstream ofs;
  pid_t pid = getpid();

  std::string pid_path = "/run/" + desc_.name + "/" + desc_.name + ".pid";

  ofs.open(pid_path.c_str(), std::ios::out | std::ios::trunc);
  ofs << pid;
  ofs.close();

  work_thread_ =
      std::jthread{[this](std::stop_token stoken) { this->workTask(stoken); }};
}

void Service::impl::stopService() {
  work_thread_.request_stop();

  if (work_thread_.joinable()) {
    work_thread_.join();
  }
}

void Service::impl::cleanResources() { stopService(); }

void Service::impl::workTask(std::stop_token stoken) {
  while (!stoken.stop_requested()) {
    std::array<std::uint8_t, ZMQ_FLATSAT_ENGINE_MTU> buf;
    int more = 0;
    std::size_t more_size = sizeof(more);

    int res = zmq_recv(engine_.sub(), buf.data(), buf.size(), 0);

    if (res < 0) {
      logs::log(ERR, "Error recv data [%s]\n", zmq_strerror(zmq_errno()));
      continue;
    }

    zmq_getsockopt(engine_.sub(), ZMQ_RCVMORE, &more, &more_size);

    if (!more) {
      logs::log(ERR, "Message is not multipart!\n");
      continue;
    }

    std::span<uint8_t> m{buf.data(), static_cast<std::size_t>(res)};

    auto request = parseMessage(buf, m, more, more_size);

    if (std::holds_alternative<std::monostate>(request)) {
      logs::log(ERR, "Failed to parse message!");
      continue;
    }

    if (std::holds_alternative<DiscoverMsgHeader>(request)) {
      logs::log(INFO, "Discover request received! Sending service details...");

      auto res = serializeServiceDescription();

      if (zmq_send(engine_.pub(), "beacon", 6U, ZMQ_SNDMORE) < 0) {
        logs::log(
            ERR,
            "Failed to send beacon topic as response to discover request!");
      }

      if (zmq_send(engine_.pub(), res.data(), res.size(), 0U) < 0) {
        logs::log(
            ERR,
            "Failed to send service data as response to discover request!");
      }
    }

    if (std::holds_alternative<Command>(request)) {
      auto command = std::get<Command>(request);

      if (!runCommandHandler(command)) {
        logs::log(ERR, "Failed to run command handler!");
      }
    }
  }
}

std::variant<std::monostate, Command, DiscoverMsgHeader, std::string>
Service::impl::parseMessage(std::span<std::uint8_t, ZMQ_FLATSAT_ENGINE_MTU> buf,
                            std::span<uint8_t> topic, int more,
                            std::size_t more_size) {
  auto min_size = std::min(g_discoverTopic.size(), desc_.name.size());

  if (topic.size() < min_size) {
    logs::log(ERR, "Message was too short to be parsed!");
    return std::monostate{};
  }

  if (topic.size() == g_discoverTopic.size()) {
    /* Check the subscribed topic of the message */

    if (std::equal(topic.begin(), topic.end(), g_discoverTopic.begin())) {
      int res = zmq_recv(engine_.sub(), buf.data(), buf.size(), 0);

      if (res < 0) {
        logs::log(ERR, "Error recv discover header [%s]\n",
                  zmq_strerror(zmq_errno()));
        return std::monostate{};
      }

      return DiscoverMsgHeader{.version = buf[0]};
    }
  }

  std::string t{reinterpret_cast<char*>(topic.data()), topic.size()};

  /* Check if the subscribed topic of the message is the service name */
  if (!std::equal(topic.begin(), topic.end(), desc_.name.begin())) {
    return t;
  }

  logs::log(DEBUG, "Received a command for service [%s]!\n", t.c_str());

  int res = zmq_recv(engine_.sub(), buf.data(), buf.size(), 0);

  if (res < 0) {
    logs::log(ERR, "Error recv command header [%s]\n",
              zmq_strerror(zmq_errno()));
    return std::monostate{};
  } else if (res != 2) {
    logs::log(ERR, "Command header must be 2 bytes\n");
    return std::monostate{};
  }

  std::span<uint8_t> raw_header{buf.data(), 2U};

  CommandMsgHeader header = {
      .version = raw_header[0],
      .proto = static_cast<MessageProtocol>(raw_header[1]),
  };

  zmq_getsockopt(engine_.sub(), ZMQ_RCVMORE, &more, &more_size);

  if (!more) {
    logs::log(ERR, "Payload is missing on multipart message!\n");
    return std::monostate{};
  }

  res = zmq_recv(engine_.sub(), buf.data(), buf.size(), 0);

  if (res < 0) {
    logs::log(ERR, "Error recv command payload [%s]\n",
              zmq_strerror(zmq_errno()));
    return std::monostate{};
  }

  std::span<const uint8_t> payload{buf.data(), static_cast<std::size_t>(res)};

  switch (header.proto) {
    case MessageProtocol::BINARY: {
      return std::monostate{};
    }
    case MessageProtocol::JSON: {
      auto parsed_cmd = parseJSON(payload);
      if (parsed_cmd.has_value()) {
        return parsed_cmd.value();
      } else {
        return std::monostate{};
      }
    }
    case MessageProtocol::PROTOBUF: {
      return std::monostate{};
    }
    default:
      throw_runtime_error("Unknown message protocol!");
  }
}

bool Service::impl::runCommandHandler(Command cmd) {
  if (!command_registry_.contains(cmd.cmd)) {
    logs::log(ERR, "Service does not suppport command [%s]!\n",
              cmd.cmd.c_str());
    return false;
  }

  auto& cmdData = command_registry_[cmd.cmd];

  for (auto& handler : cmdData.handlers) {
    if (handler.first != nullptr) {
      handler.first(handler.second, cmd);
    }
  }

  return true;
}

std::vector<char> Service::impl::serializeServiceDescription() {
  using json = nlohmann::json;

  json j;

  j["name"] = desc_.name;
  j["version"] = desc_.version;
  j["compatible_protocols"] =
      protoToString(static_cast<MessageProtocol>(desc_.compatibleProtocols));

  json cmd_array = json::array();

  for (const auto& cmd : command_registry_) {
    json c;
    json arg_array = json::array();

    c["name"] = cmd.first;

    for (const auto& arg : cmd.second.args) {
      json a;

      a["name"] = arg.name;
      a["type"] = typeToString(arg.type);
      a["optional"] = arg.optional;

      arg_array.push_back(a);
    }

    c["args"] = arg_array;

    cmd_array.push_back(c);
  }

  j["commands"] = cmd_array;

  std::string json_str = j.dump();

  return {json_str.begin(), json_str.end()};
}

bool Service::impl::connectToEngineProxy() {
  return (engine_.configure_zprotocol(desc_.name) == 0) ? true : false;
}

bool Service::impl::registerCommand(CommandType command,
                                    std::vector<CommandArg> args,
                                    std::optional<CommandHandlerFn> handler,
                                    std::optional<void*> handlerData) {
  void* data = handlerData.value_or(nullptr);
  auto fn = handler.value_or(nullptr);

  if (command_registry_.contains(command)) {
    if (fn != nullptr) {
      auto& r = command_registry_[command];
      r.handlers.push_back({fn, data});
    }

    return true;
  }

  RegistryData first_reg = {
      .args = args,
      .handlers = {{fn, data}},
  };

  auto res = command_registry_.emplace(command, first_reg);

  return res.second;
}

bool Service::impl::registerHandler(CommandType& command,
                                    Service::CommandHandlerFn handler,
                                    void* handlerData) {
  if (!command_registry_.contains(command)) return false;

  auto& reg = command_registry_[command];

  reg.handlers.push_back({handler, handlerData});

  return true;
}

bool Service::impl::subscribeTo(std::string_view topic) {
  return (engine_.subscribe_to(topic) == 0) ? true : false;
}

bool Service::impl::publishRawBytes(std::string_view topic,
                                    std::span<std::uint8_t> data) {
  return (engine_.publish_raw_bytes(topic, data) == 0) ? true : false;
}

}  // namespace zmq

}  // namespace fsatutils
