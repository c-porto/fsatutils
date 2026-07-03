#include <iio.h>

#include <array>
#include <fsatutils/errors.hpp>
#include <fsatutils/iio/device.hpp>
#include <memory>
#include <string>

namespace fsatutils {

namespace iio {

Device::Device(std::shared_ptr<Context> ctx, std::string name)
    : ctx_{ctx}, name_{std::move(name)} {
  raw_ = iio_context_find_device(*ctx_, name_.c_str());

  if (raw_ == nullptr) {
    throw_runtime_error("Failed to create " + name_ + " IIO Device!");
  }
}

Channel Device::find_device_channel(std::string const& channel_name,
                                    bool output) {
  auto ch = iio_device_find_channel(raw_, channel_name.c_str(), output);

  if (ch == nullptr) {
    throw_runtime_error("Failed to find " + channel_name + " IIO Channel!");
  }

  return {channel_name, raw_, output};
}

template <>
void Device::write_attr(std::string const& attr, long long const& value) {
  int res = iio_device_attr_write_longlong(raw_, attr.c_str(), value);
  if (res < 0) {
    throw_runtime_error("Failed to write " + attr + " Device attribute!");
  }
}

template <>
void Device::write_attr(std::string const& attr, bool const& value) {
  int res = iio_device_attr_write_bool(raw_, attr.c_str(), value);
  if (res < 0) {
    throw_runtime_error("Failed to write " + attr + " Device attribute!");
  }
}

template <>
void Device::write_attr(std::string const& attr, double const& value) {
  int res = iio_device_attr_write_double(raw_, attr.c_str(), value);
  if (res < 0) {
    throw_runtime_error("Failed to write " + attr + " Device attribute!");
  }
}

template <>
void Device::write_attr(std::string const& attr, std::string const& value) {
  int res = iio_device_attr_write(raw_, attr.c_str(), value.c_str());

  if ((res < 0) || (res != static_cast<int>(value.length() + 1))) {
    throw_runtime_error("Failed to write " + attr + " Device attribute!");
  }
}

template <>
long long Device::read_attr(std::string const& attr) const {
  long long val = 0;

  int res = iio_device_attr_read_longlong(raw_, attr.c_str(), &val);

  if (res < 0) {
    throw_runtime_error("Failed to read " + attr + " Device attribute!");
  }

  return val;
}

template <>
std::string Device::read_attr(std::string const& attr) const {
  std::array<char, 1024U> buf;

  int res = iio_device_attr_read(raw_, attr.c_str(), buf.data(), buf.size());

  if (res < 0) {
    throw_runtime_error("Failed to read " + attr + " Device attribute!");
  }

  return {buf.data()};
}

template <>
bool Device::read_attr(std::string const& attr) const {
  bool val;

  int res = iio_device_attr_read_bool(raw_, attr.c_str(), &val);

  if (res < 0) {
    throw_runtime_error("Failed to read " + attr + " Device attribute!");
  }

  return val;
}

template <>
double Device::read_attr(std::string const& attr) const {
  double val;

  int res = iio_device_attr_read_double(raw_, attr.c_str(), &val);

  if (res < 0) {
    throw_runtime_error("Failed to read " + attr + " Device attribute!");
  }

  return val;
}

template <>
void Device::write_debug_attr(std::string const& attr, long long const& value) {
  int res = iio_device_debug_attr_write_longlong(raw_, attr.c_str(), value);
  if (res < 0) {
    throw_runtime_error("Failed to write " + attr + " Device debug attribute!");
  }
}

template <>
void Device::write_debug_attr(std::string const& attr, bool const& value) {
  int res = iio_device_debug_attr_write_bool(raw_, attr.c_str(), value);
  if (res < 0) {
    throw_runtime_error("Failed to write " + attr + " Device debug attribute!");
  }
}

template <>
void Device::write_debug_attr(std::string const& attr, double const& value) {
  int res = iio_device_debug_attr_write_double(raw_, attr.c_str(), value);
  if (res < 0) {
    throw_runtime_error("Failed to write " + attr + " Device debug attribute!");
  }
}

template <>
void Device::write_debug_attr(std::string const& attr,
                              std::string const& value) {
  int res = iio_device_debug_attr_write(raw_, attr.c_str(), value.c_str());

  if ((res < 0) || (res != static_cast<int>(value.length() + 1))) {
    throw_runtime_error("Failed to write " + attr + " Device debug attribute!");
  }
}

template <>
long long Device::read_debug_attr(std::string const& attr) const {
  long long val = 0;

  int res = iio_device_debug_attr_read_longlong(raw_, attr.c_str(), &val);

  if (res < 0) {
    throw_runtime_error("Failed to read " + attr + " Device debug attribute!");
  }

  return val;
}

template <>
std::string Device::read_debug_attr(std::string const& attr) const {
  std::array<char, 1024U> buf;

  int res =
      iio_device_debug_attr_read(raw_, attr.c_str(), buf.data(), buf.size());

  if (res < 0) {
    throw_runtime_error("Failed to read " + attr + " Device debug attribute!");
  }

  return {buf.data()};
}

template <>
bool Device::read_debug_attr(std::string const& attr) const {
  bool val;

  int res = iio_device_debug_attr_read_bool(raw_, attr.c_str(), &val);

  if (res < 0) {
    throw_runtime_error("Failed to read " + attr + " Device debug attribute!");
  }

  return val;
}

template <>
double Device::read_debug_attr(std::string const& attr) const {
  double val;

  int res = iio_device_debug_attr_read_double(raw_, attr.c_str(), &val);

  if (res < 0) {
    throw_runtime_error("Failed to read " + attr + " Device debug attribute!");
  }

  return val;
}

}  // namespace iio

}  // namespace fsatutils
