cmake_minimum_required(VERSION 3.26)
project(In_NetworkComputing)

set(CMAKE_CXX_STANDARD 20)

include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/source)

set(SOURCES
    source/main.cpp
    source/Network/Port.cpp
    source/Network/Switches/Core.cpp
    source/Network/Switches/Aggregate.cpp
    source/Network/Switches/ISwitch.cpp
    source/Network/Switches/Edge.cpp
    source/Network/Switches/InterSwitchMessages.cpp
    source/Computer.cpp
    source/Network/MPI.cpp
    source/Network/Message.cpp
    source/Network/Constants.cpp
)

set(HEADERS
    source/cxxopts.hpp
    source/Network/Port.hpp
    source/Network/Switches/Core.hpp
    source/Network/Switches/Aggregate.hpp
    source/Network/Switches/ISwitch.hpp
    source/Network/Switches/Edge.hpp
    source/Network/Switches/InterSwitchMessages.hpp
    source/Computer.hpp
    source/Network/MPI.hpp
    source/Network/Message.hpp
    source/Network/Constants.hpp
)

find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)

add_executable(${CMAKE_PROJECT_NAME} ${SOURCES} ${HEADERS})

target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE spdlog)
target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE fmt)
