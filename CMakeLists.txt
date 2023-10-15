cmake_minimum_required(VERSION 3.26)
project(In_NetworkComputing)

set(CMAKE_CXX_STANDARD 17)

include_directories(${CMAKE_SOURCE_DIR})
add_subdirectory(spdlog)
add_subdirectory(cxxopts)

add_executable(In_NetworkComputing main.cpp
        Network/Port.cpp
        Network/Port.hpp
        Network/Switches/Core.cpp
        Network/Switches/Core.hpp
        Network/Switches/Aggregate.cpp
        Network/Switches/Aggregate.hpp
        Network/Switches/ISwitch.cpp
        Network/Switches/ISwitch.hpp
        Network/Switches/Edge.cpp
        Network/Switches/Edge.hpp
        Computer.cpp
        Computer.hpp
)

target_link_libraries(In_NetworkComputing PRIVATE spdlog)
target_link_libraries(In_NetworkComputing PRIVATE cxxopts)
