#ifndef PARAMETER_HPP_
#define PARAMETER_HPP_

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

namespace fsatutils {

class ParameterSystem {
 public:
  using ByteArray = std::vector<std::uint8_t>;
  using Value =
      std::variant<bool, std::int8_t, std::uint8_t, std::int16_t, std::uint16_t,
                   std::int32_t, std::uint32_t, std::int64_t, std::uint64_t,
                   float, double, std::string, ByteArray>;

  enum class Type : std::uint8_t {
    BOOL,
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    UINT64,
    FLOAT,
    DOUBLE,
    STRING,
    BYTE_ARRAY,
  };

  struct Description {
    std::string name;
    Type type;
    bool writable;
  };

  bool declare(std::string name, bool& value, bool writable = true);
  bool declare(std::string name, std::int8_t& value, bool writable = true);
  bool declare(std::string name, std::uint8_t& value, bool writable = true);
  bool declare(std::string name, std::int16_t& value, bool writable = true);
  bool declare(std::string name, std::uint16_t& value, bool writable = true);
  bool declare(std::string name, std::int32_t& value, bool writable = true);
  bool declare(std::string name, std::uint32_t& value, bool writable = true);
  bool declare(std::string name, std::int64_t& value, bool writable = true);
  bool declare(std::string name, std::uint64_t& value, bool writable = true);
  bool declare(std::string name, float& value, bool writable = true);
  bool declare(std::string name, double& value, bool writable = true);
  bool declare(std::string name, std::string& value, bool writable = true);
  bool declare(std::string name, ByteArray& value, bool writable = true);

  std::optional<Value> read(std::string_view name) const;
  bool write(std::string_view name, Value const& value);

  bool contains(std::string_view name) const;
  bool isWritable(std::string_view name) const;
  std::vector<Description> describe() const;

 private:
  using Reference = std::variant<
      std::reference_wrapper<bool>, std::reference_wrapper<std::int8_t>,
      std::reference_wrapper<std::uint8_t>,
      std::reference_wrapper<std::int16_t>,
      std::reference_wrapper<std::uint16_t>,
      std::reference_wrapper<std::int32_t>,
      std::reference_wrapper<std::uint32_t>,
      std::reference_wrapper<std::int64_t>,
      std::reference_wrapper<std::uint64_t>, std::reference_wrapper<float>,
      std::reference_wrapper<double>, std::reference_wrapper<std::string>,
      std::reference_wrapper<ByteArray>>;

  struct Entry {
    Reference reference;
    bool writable;
  };

  template <typename T>
  bool declareImpl(std::string name, T& value, bool writable);

  static Type typeOf(Reference const& reference);

  std::unordered_map<std::string, Entry> parameters_;
};

}  // namespace fsatutils

#endif
