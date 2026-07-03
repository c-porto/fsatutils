#include <array>
#include <fsatutils/zmq/parameter_protocol.hpp>
#include <nlohmann/json.hpp>
#include <type_traits>

namespace fsatutils {

namespace zmq {

namespace {

using json = nlohmann::json;

constexpr std::array<std::string_view, 13U> g_parameterTypes = {
    "bool", "i8",  "u8",    "i16",    "u16",    "i32",   "u32",
    "i64",  "u64", "float", "double", "string", "bytes",
};

std::optional<ParameterSystem::Type> parseParameterType(std::string_view type) {
  for (std::size_t index = 0; index < g_parameterTypes.size(); ++index) {
    if (g_parameterTypes[index] == type) {
      return static_cast<ParameterSystem::Type>(index);
    }
  }

  return std::nullopt;
}

json valueToJson(ParameterSystem::Value const& value) {
  return std::visit(
      [](auto const& item) -> json {
        using T = std::decay_t<decltype(item)>;

        if constexpr (std::is_same_v<T, std::int8_t>) {
          return static_cast<std::int32_t>(item);
        } else if constexpr (std::is_same_v<T, std::uint8_t>) {
          return static_cast<std::uint32_t>(item);
        } else {
          return item;
        }
      },
      value);
}

std::optional<ParameterSystem::Value> valueFromJson(ParameterSystem::Type type,
                                                    json const& value) {
  try {
    switch (type) {
      case ParameterSystem::Type::BOOL:
        return value.get<bool>();
      case ParameterSystem::Type::INT8:
        return value.get<std::int8_t>();
      case ParameterSystem::Type::UINT8:
        return value.get<std::uint8_t>();
      case ParameterSystem::Type::INT16:
        return value.get<std::int16_t>();
      case ParameterSystem::Type::UINT16:
        return value.get<std::uint16_t>();
      case ParameterSystem::Type::INT32:
        return value.get<std::int32_t>();
      case ParameterSystem::Type::UINT32:
        return value.get<std::uint32_t>();
      case ParameterSystem::Type::INT64:
        return value.get<std::int64_t>();
      case ParameterSystem::Type::UINT64:
        return value.get<std::uint64_t>();
      case ParameterSystem::Type::FLOAT:
        return value.get<float>();
      case ParameterSystem::Type::DOUBLE:
        return value.get<double>();
      case ParameterSystem::Type::STRING:
        return value.get<std::string>();
      case ParameterSystem::Type::BYTE_ARRAY:
        return value.get<ParameterSystem::ByteArray>();
    }
  } catch (json::exception const&) {
    return std::nullopt;
  }

  return std::nullopt;
}

ParameterSystem::Type typeOf(ParameterSystem::Value const& value) {
  return static_cast<ParameterSystem::Type>(value.index());
}

std::optional<std::pair<std::string, std::string>> parseTopic(
    std::string_view topic, std::string_view separator) {
  auto position = topic.find(separator);
  if (position == std::string_view::npos) return std::nullopt;

  auto service = topic.substr(0U, position);
  auto name = topic.substr(position + separator.size());

  if (service.empty() || name.empty()) return std::nullopt;

  return std::pair{std::string{service}, std::string{name}};
}

std::vector<std::uint8_t> serializeJson(json const& object) {
  auto serialized = object.dump();
  return {serialized.begin(), serialized.end()};
}

}  // namespace

std::string_view parameterTypeToString(ParameterSystem::Type type) {
  auto index = static_cast<std::size_t>(type);
  if (index >= g_parameterTypes.size()) return "invalid";
  return g_parameterTypes[index];
}

std::string parameterTopic(std::string_view service, std::string_view name) {
  return std::string{service} + "/param/" + std::string{name};
}

std::string parameterControlTopic(std::string_view service,
                                  std::string_view name) {
  return std::string{service} + "/param-control/" + std::string{name};
}

std::vector<std::uint8_t> serializeParameterValue(
    ParameterSystem::Value const& value) {
  json object;
  object["type"] = parameterTypeToString(typeOf(value));
  object["value"] = valueToJson(value);
  return serializeJson(object);
}

std::vector<std::uint8_t> serializeParameterControl(
    ParameterOperation operation,
    std::optional<ParameterSystem::Value> const& value) {
  json object;
  object["operation"] = operation == ParameterOperation::GET ? "get" : "set";

  if (value.has_value()) {
    object["type"] = parameterTypeToString(typeOf(*value));
    object["value"] = valueToJson(*value);
  }

  return serializeJson(object);
}

std::optional<ParameterUpdate> parseParameterUpdate(
    std::string_view topic, std::span<const std::uint8_t> payload) {
  auto parsed_topic = parseTopic(topic, "/param/");
  if (!parsed_topic.has_value()) return std::nullopt;

  try {
    auto object = json::parse(payload);
    auto type = parseParameterType(object.at("type").get<std::string>());
    if (!type.has_value()) return std::nullopt;

    auto value = valueFromJson(*type, object.at("value"));
    if (!value.has_value()) return std::nullopt;

    return ParameterUpdate{
        .service = std::move(parsed_topic->first),
        .name = std::move(parsed_topic->second),
        .value = std::move(*value),
    };
  } catch (json::exception const&) {
    return std::nullopt;
  }
}

std::optional<ParameterControl> parseParameterControl(
    std::string_view topic, std::span<const std::uint8_t> payload) {
  auto parsed_topic = parseTopic(topic, "/param-control/");
  if (!parsed_topic.has_value()) return std::nullopt;

  try {
    auto object = json::parse(payload);
    auto operation = object.at("operation").get<std::string>();

    if (operation == "get") {
      return ParameterControl{
          .service = std::move(parsed_topic->first),
          .name = std::move(parsed_topic->second),
          .operation = ParameterOperation::GET,
          .value = std::nullopt,
      };
    }

    if (operation != "set") return std::nullopt;

    auto type = parseParameterType(object.at("type").get<std::string>());
    if (!type.has_value()) return std::nullopt;

    auto value = valueFromJson(*type, object.at("value"));
    if (!value.has_value()) return std::nullopt;

    return ParameterControl{
        .service = std::move(parsed_topic->first),
        .name = std::move(parsed_topic->second),
        .operation = ParameterOperation::SET,
        .value = std::move(value),
    };
  } catch (json::exception const&) {
    return std::nullopt;
  }
}

}  // namespace zmq

}  // namespace fsatutils
