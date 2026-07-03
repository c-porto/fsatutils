#include <zmq.h>

#include <array>
#include <cassert>
#include <cstring>
#include <fsatutils/errors.hpp>
#include <fsatutils/log/log.hpp>
#include <fsatutils/parameter/parameter.hpp>
#include <fsatutils/zmq/parameter_protocol.hpp>
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

  bool exposeParameters(ParameterSystem& parameters);
  bool publishParameter(std::string_view name);

 private:
  std::variant<std::monostate, Command, DiscoverMsgHeader, ParameterControl,
               std::string>
  parseMessage(std::span<std::uint8_t, ZMQ_FLATSAT_ENGINE_MTU> buf,
               std::span<uint8_t> topic, int more, std::size_t more_size);

  bool runCommandHandler(Command cmd);
  bool runParameterControl(ParameterControl const& control);

  std::vector<char> serializeServiceDescription();

  bool connectToEngineProxy();

  ServiceDescription desc_;
  ZMQEngine engine_;
  std::unordered_map<CommandType, RegistryData> command_registry_;
  std::jthread work_thread_;
  ParameterSystem* parameters_ = nullptr;
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

bool Service::exposeParameters(ParameterSystem& parameters) {
  return impl_->exposeParameters(parameters);
}

bool Service::publishParameter(std::string_view name) {
  return impl_->publishParameter(name);
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
  if (parameters_ != nullptr) {
    for (auto const& parameter : parameters_->describe()) {
      publishParameter(parameter.name);
    }
  }

  while (!stoken.stop_requested()) {
    std::array<std::uint8_t, ZMQ_FLATSAT_ENGINE_MTU> buf;
    int more = 0;
    std::size_t more_size = sizeof(more);

    zmq_pollitem_t item = {
        .socket = engine_.sub(), .fd = 0, .events = ZMQ_POLLIN, .revents = 0};

    int poll_result = zmq_poll(&item, 1, 100);

    if (poll_result < 0) {
      logs::log(ERR, "Error polling service socket [%s]\n",
                zmq_strerror(zmq_errno()));
      continue;
    }

    if (poll_result == 0 || !(item.revents & ZMQ_POLLIN)) continue;

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

    if (std::holds_alternative<ParameterControl>(request)) {
      auto const& control = std::get<ParameterControl>(request);

      if (!runParameterControl(control)) {
        logs::log(ERR, "Failed to process parameter [%s]!\n",
                  control.name.c_str());
      }
    }
  }
}

std::variant<std::monostate, Command, DiscoverMsgHeader, ParameterControl,
             std::string>
Service::impl::parseMessage(std::span<std::uint8_t, ZMQ_FLATSAT_ENGINE_MTU> buf,
                            std::span<uint8_t> topic, int more,
                            std::size_t more_size) {
  std::string topic_name{reinterpret_cast<char*>(topic.data()), topic.size()};

  if (topic_name == g_discoverTopic) {
    int res = zmq_recv(engine_.sub(), buf.data(), buf.size(), 0);

    if (res < 0) {
      logs::log(ERR, "Error recv discover header [%s]\n",
                zmq_strerror(zmq_errno()));
      return std::monostate{};
    }

    return DiscoverMsgHeader{.version = buf[0]};
  }

  auto parameter_control_prefix = desc_.name + "/param-control/";

  if (topic_name.starts_with(parameter_control_prefix)) {
    int res = zmq_recv(engine_.sub(), buf.data(), buf.size(), 0);

    if (res < 0) {
      logs::log(ERR, "Error recv parameter control payload [%s]\n",
                zmq_strerror(zmq_errno()));
      return std::monostate{};
    }

    std::span<const std::uint8_t> payload{buf.data(),
                                          static_cast<std::size_t>(res)};
    auto control = parseParameterControl(topic_name, payload);

    if (!control.has_value()) {
      logs::log(ERR, "Failed to parse parameter control message!\n");
      return std::monostate{};
    }

    return std::move(*control);
  }

  if (topic_name != desc_.name) {
    while (more) {
      int res = zmq_recv(engine_.sub(), buf.data(), buf.size(), 0);

      if (res < 0) {
        logs::log(ERR, "Error draining message payload [%s]\n",
                  zmq_strerror(zmq_errno()));
        return std::monostate{};
      }

      zmq_getsockopt(engine_.sub(), ZMQ_RCVMORE, &more, &more_size);
    }

    return topic_name;
  }

  logs::log(DEBUG, "Received a command for service [%s]!\n",
            topic_name.c_str());

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

bool Service::impl::runParameterControl(ParameterControl const& control) {
  if (parameters_ == nullptr || control.service != desc_.name ||
      !parameters_->contains(control.name)) {
    return false;
  }

  if (control.operation == ParameterOperation::SET) {
    if (!parameters_->isWritable(control.name) || !control.value.has_value() ||
        !parameters_->write(control.name, *control.value)) {
      return false;
    }
  }

  return publishParameter(control.name);
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

  json parameter_array = json::array();

  if (parameters_ != nullptr) {
    for (auto const& parameter : parameters_->describe()) {
      json p;
      p["name"] = parameter.name;
      p["type"] = parameterTypeToString(parameter.type);
      p["writable"] = parameter.writable;
      parameter_array.push_back(p);
    }
  }

  j["parameters"] = parameter_array;

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

bool Service::impl::exposeParameters(ParameterSystem& parameters) {
  if (parameters_ != nullptr || work_thread_.joinable()) return false;

  parameters_ = &parameters;
  return true;
}

bool Service::impl::publishParameter(std::string_view name) {
  if (parameters_ == nullptr) return false;

  auto value = parameters_->read(name);
  if (!value.has_value()) return false;

  auto topic = parameterTopic(desc_.name, name);
  auto payload = serializeParameterValue(*value);

  return publishRawBytes(topic, payload);
}

}  // namespace zmq

}  // namespace fsatutils
