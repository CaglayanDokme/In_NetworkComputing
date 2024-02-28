cmake_minimum_required(VERSION 3.26)
project(In_NetworkComputing)

set(CMAKE_CXX_STANDARD 20)

include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/source)
add_subdirectory(cxxopts)

set(SOURCES
    source/main.cpp
    source/Network/Port.cpp
    source/Network/Switches/Core.cpp
    source/Network/Switches/Aggregate.cpp
    source/Network/Switches/ISwitch.cpp
    source/Network/Switches/Edge.cpp
    source/Computer.cpp
    source/Network/Message.cpp
    source/Network/Derivations.hpp
)

set(HEADERS
    source/Network/Port.hpp
    source/Network/Switches/Core.hpp
    source/Network/Switches/Aggregate.hpp
    source/Network/Switches/ISwitch.hpp
    source/Network/Switches/Edge.hpp
    source/Computer.hpp
    source/Network/Message.hpp
)

find_package(spdlog REQUIRED)

add_executable(${CMAKE_PROJECT_NAME} ${SOURCES} ${HEADERS})

target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE spdlog)
target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE cxxopts)
