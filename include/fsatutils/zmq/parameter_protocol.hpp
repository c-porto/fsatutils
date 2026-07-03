#ifndef PARAMETER_PROTOCOL_HPP_
#define PARAMETER_PROTOCOL_HPP_

#include <cstdint>
#include <fsatutils/parameter/parameter.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace fsatutils {

namespace zmq {

enum class ParameterOperation : std::uint8_t {
  GET,
  SET,
};

struct ParameterUpdate {
  std::string service;
  std::string name;
  ParameterSystem::Value value;
};

struct ParameterControl {
  std::string service;
  std::string name;
  ParameterOperation operation;
  std::optional<ParameterSystem::Value> value;
};

std::string_view parameterTypeToString(ParameterSystem::Type type);

std::string parameterTopic(std::string_view service, std::string_view name);
std::string parameterControlTopic(std::string_view service,
                                  std::string_view name);

std::vector<std::uint8_t> serializeParameterValue(
    ParameterSystem::Value const& value);
std::vector<std::uint8_t> serializeParameterControl(
    ParameterOperation operation,
    std::optional<ParameterSystem::Value> const& value = std::nullopt);

std::optional<ParameterUpdate> parseParameterUpdate(
    std::string_view topic, std::span<const std::uint8_t> payload);
std::optional<ParameterControl> parseParameterControl(
    std::string_view topic, std::span<const std::uint8_t> payload);

}  // namespace zmq

}  // namespace fsatutils

#endif
