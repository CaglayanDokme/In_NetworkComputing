#include "InterSwitchMessages.hpp"

using namespace Network::Messages::InterSwitch;

Gather::Gather(const std::size_t destinationID)
: BaseMessage(e_Type::IS_Gather), m_destinationID(destinationID)
{
    // Nothing
}
