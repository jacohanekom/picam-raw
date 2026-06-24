cmake_minimum_required(VERSION 3.16)
project(picam_raw CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBCAMERA REQUIRED libcamera)

add_executable(picam_raw picam_raw.cpp)

target_include_directories(picam_raw PRIVATE ${LIBCAMERA_INCLUDE_DIRS})
target_link_libraries(picam_raw PRIVATE ${LIBCAMERA_LIBRARIES} pthread)
target_compile_options(picam_raw PRIVATE ${LIBCAMERA_CFLAGS_OTHER} -Wall -Wextra -O2)

install(TARGETS picam_raw DESTINATION bin)
