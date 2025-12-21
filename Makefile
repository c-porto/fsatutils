BUILD_DIR = build
MAKEFLAGS += --no-print-directory

.Phony:
all: meson.build
	@meson setup --prefix=/usr $(BUILD_DIR)
	@meson compile -C $(BUILD_DIR)

.Phony:
clean: meson.build
	@ninja clean -C $(BUILD_DIR)

.Phony:
install: meson.build
	@meson install -C $(BUILD_DIR)
