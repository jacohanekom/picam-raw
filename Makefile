# Makefile — picam_rtsp (Raspberry Pi 5, Pi OS Bookworm)
#
# Requires:
#   sudo apt install libcamera-dev libavcodec-dev libavutil-dev libx264-dev
#
# Usage:
#   make          — build ./picam_rtsp
#   make install  — copy binary to /usr/local/bin
#   make clean    — remove build artefacts

CXX      := g++
TARGET   := picam_rtsp
SRCS     := picam_rtsp.cpp
HDRS     := picam_rtsp.hpp

# pkg-config pulls the right include paths and linker flags for each library
PKGS     := libcamera libavcodec libavutil

PKG_CFLAGS  := $(shell pkg-config --cflags $(PKGS))
PKG_LIBS    := $(shell pkg-config --libs   $(PKGS))

CXXFLAGS := -std=c++20 -O2 -Wall -Wextra -pthread $(PKG_CFLAGS)
LDFLAGS  := -pthread $(PKG_LIBS)

.PHONY: all clean install

all: $(TARGET)

$(TARGET): $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) -o $@ $(SRCS) $(LDFLAGS)

install: $(TARGET)
	install -m 755 $(TARGET) /usr/local/bin/$(TARGET)

clean:
	rm -f $(TARGET)
