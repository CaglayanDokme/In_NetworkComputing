#include "Core.hpp"
#include "spdlog/spdlog.h"

using namespace Network::Switches;

Core::Core(const std::size_t portAmount)
: ISwitch(nextID++, portAmount)
{
    spdlog::trace("Created core switch with ID #{}", m_ID);
}

bool Core::tick()
{
    return false;
}
