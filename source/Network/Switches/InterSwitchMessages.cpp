#include "InterSwitchMessages.hpp"

using namespace Network::Messages::InterSwitch;

Reduce::Reduce(const size_t destinationID)
: BaseMessage(e_Type::IS_Reduce, std::nullopt, destinationID)
{
    // Nothing
}

size_t Reduce::size() const
{
    const auto size = BaseMessage::size() + (sizeof(m_contributors) + (m_contributors.size() * sizeof(m_contributors.front()))) + (sizeof(m_data) + (m_data.size() * sizeof(m_data.front())));

    return size;
}

Scatter::Scatter(const size_t sourceID)
: BaseMessage(e_Type::IS_Scatter, sourceID)
{
    // Nothing
}

size_t Scatter::size() const
{
    auto size = BaseMessage::size();

    size += sizeof(m_data);

    for(const auto &pair : m_data) {
        size += sizeof(pair.first) + (pair.second.size() * sizeof(pair.second.front()));
    }

    return size;
}

Gather::Gather(const size_t destinationID)
: BaseMessage(e_Type::IS_Gather, std::nullopt, destinationID)
{
    // Nothing
}

size_t Gather::size() const
{
    auto size = BaseMessage::size();

    size += sizeof(m_data);

    for(const auto &pair : m_data) {
        size += sizeof(pair.first) + (pair.second.size() * sizeof(pair.second.front()));
    }

    return size;
}

AllGather::AllGather()
: BaseMessage(e_Type::IS_AllGather)
{
    // Nothing
}

size_t AllGather::size() const
{
    auto size = BaseMessage::size();

    size += sizeof(m_data);

    for(const auto &pair : m_data) {
        size += sizeof(pair.first) + (pair.second.size() * sizeof(pair.second.front()));
    }

    return size;
}
