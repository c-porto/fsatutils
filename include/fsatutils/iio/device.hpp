#ifndef DEVICE_HPP_
#define DEVICE_HPP_

#include <iio.h>

#include <fsatutils/iio/channel.hpp>
#include <fsatutils/iio/context.hpp>
#include <memory>
#include <string>

namespace fsatutils {

namespace iio {

class Device {
 public:
  Device(std::shared_ptr<Context> ctx, std::string name);

  Channel find_device_channel(std::string const& channel_name, bool output);

  template <typename AttrType>
  void write_attr(std::string const& attr, AttrType const& value);
  template <typename AttrType>
  AttrType read_attr(std::string const& attr) const;

  template <typename AttrType>
  void write_debug_attr(std::string const& attr, AttrType const& value);
  template <typename AttrType>
  AttrType read_debug_attr(std::string const& attr) const;

  std::string name() const noexcept { return name_; }

  operator struct iio_device*() { return raw_; };

 private:
  std::shared_ptr<Context> ctx_;
  struct iio_device* raw_;
  std::string name_;
};

}  // namespace iio

}  // namespace fsatutils

#endif
