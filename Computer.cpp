#include "Computer.hpp"
#include "spdlog/spdlog.h"

Computer::Computer()
: m_ID(nextID++)
{
    spdlog::trace("Created computing node with ID #{}", m_ID);
}

bool Computer::tick()
{
    return false;
}

bool Computer::isReady() const
{
    return m_port.isConnected();
}
