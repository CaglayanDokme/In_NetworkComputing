#include "InterSwitchMessages.hpp"

using namespace Network::Messages::InterSwitch;

Reduce::Reduce(const size_t destinationID)
: BaseMessage(e_Type::IS_Reduce, std::nullopt, destinationID)
{
    // Nothing
}

Scatter::Scatter(const size_t sourceID)
: BaseMessage(e_Type::IS_Scatter, sourceID)
{
    // Nothing
}

Gather::Gather(const size_t destinationID)
: BaseMessage(e_Type::IS_Gather, std::nullopt, destinationID)
{
    // Nothing
}

AllGather::AllGather()
: BaseMessage(e_Type::IS_AllGather)
{
    // Nothing
}
