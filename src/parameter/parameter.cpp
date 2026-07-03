#include <fsatutils/parameter/parameter.hpp>
#include <utility>

namespace fsatutils {

template <typename T>
bool ParameterSystem::declareImpl(std::string name, T& value, bool writable) {
  if (name.empty() || parameters_.contains(name)) return false;

  return parameters_
      .emplace(std::move(name),
               Entry{.reference = std::ref(value), .writable = writable})
      .second;
}

bool ParameterSystem::declare(std::string name, bool& value, bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, std::int8_t& value,
                              bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, std::uint8_t& value,
                              bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, std::int16_t& value,
                              bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, std::uint16_t& value,
                              bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, std::int32_t& value,
                              bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, std::uint32_t& value,
                              bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, std::int64_t& value,
                              bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, std::uint64_t& value,
                              bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, float& value, bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, double& value, bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, std::string& value,
                              bool writable) {
  return declareImpl(std::move(name), value, writable);
}

bool ParameterSystem::declare(std::string name, ByteArray& value,
                              bool writable) {
  return declareImpl(std::move(name), value, writable);
}

std::optional<ParameterSystem::Value> ParameterSystem::read(
    std::string_view name) const {
  auto parameter = parameters_.find(std::string{name});
  if (parameter == parameters_.end()) return std::nullopt;

  return std::visit([](auto reference) -> Value { return reference.get(); },
                    parameter->second.reference);
}

bool ParameterSystem::write(std::string_view name, Value const& value) {
  auto parameter = parameters_.find(std::string{name});
  if (parameter == parameters_.end()) return false;

  return std::visit(
      [&value](auto reference) {
        using T = typename decltype(reference)::type;
        auto new_value = std::get_if<T>(&value);

        if (new_value == nullptr) return false;

        reference.get() = *new_value;
        return true;
      },
      parameter->second.reference);
}

bool ParameterSystem::contains(std::string_view name) const {
  return parameters_.contains(std::string{name});
}

bool ParameterSystem::isWritable(std::string_view name) const {
  auto parameter = parameters_.find(std::string{name});
  return parameter != parameters_.end() && parameter->second.writable;
}

std::vector<ParameterSystem::Description> ParameterSystem::describe() const {
  std::vector<Description> descriptions;
  descriptions.reserve(parameters_.size());

  for (auto const& [name, parameter] : parameters_) {
    descriptions.push_back({
        .name = name,
        .type = typeOf(parameter.reference),
        .writable = parameter.writable,
    });
  }

  return descriptions;
}

ParameterSystem::Type ParameterSystem::typeOf(Reference const& reference) {
  return static_cast<Type>(reference.index());
}

}  // namespace fsatutils
