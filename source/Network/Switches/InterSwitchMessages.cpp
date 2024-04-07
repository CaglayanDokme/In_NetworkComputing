#include "InterSwitchMessages.hpp"

using namespace Network::Messages::InterSwitch;

Scatter::Scatter(const std::size_t sourceID)
: BaseMessage(e_Type::IS_Scatter), m_sourceID(sourceID)
{
    // Nothing
}

Gather::Gather(const std::size_t destinationID)
: BaseMessage(e_Type::IS_Gather), m_destinationID(destinationID)
{
    // Nothing
}
