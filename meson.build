project(
  'fsatutils',
  'cpp',
  version: '0.2',
  default_options: [
    'warning_level=3',
    'cpp_std=c++20',
    'optimization=3',
    'buildtype=release',
  ],
)

fsatutils_deps = []
fsatutils_srcs = []

fsatutils_inc = include_directories('include')

libiio = dependency('libiio')

zmq = dependency('libzmq')

nlohmann_json = dependency('nlohmann_json')

fsatutils_deps += libiio
fsatutils_deps += zmq
fsatutils_deps += nlohmann_json

subdir('src')

c_args = [
  '-Wno-unused-parameter',
  '-Wno-unused-value',
  '-Wno-missing-field-initializers',
  '-Wno-narrowing',
  '-Wno-pointer-arith',
  '-Wshadow',
  '-Wcast-align',
  '-Wwrite-strings',
]

add_project_arguments(
  [
    '-Wno-unused-parameter',
    '-Wno-unused-value',
    '-Wno-missing-field-initializers',
    '-Wno-narrowing',
    '-Wno-pointer-arith',
  ],
  language: 'cpp',
)

fsatutils = shared_library(
  meson.project_name(),
  sources: fsatutils_srcs,
  include_directories: fsatutils_inc,
  c_args: c_args,
  install: true,
  version: meson.project_version(),
  soversion: meson.project_version().split('.')[0],
  dependencies: fsatutils_deps,
)

fsatutils_dep = declare_dependency(link_with: fsatutils, include_directories: include_directories('include'))

install_subdir(
  'include/fsatutils',
  install_dir: get_option('includedir'),
)

pkg = import('pkgconfig')

pkg.generate(
  libraries: fsatutils,
  name: 'fsatutils',
  description: 'FlatSat2 utilities library',
  version: meson.project_version(),
  subdirs: 'fsatutils',
  requires: ['libiio', 'libzmq', 'nlohmann_json'],
)
