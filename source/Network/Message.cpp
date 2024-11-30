#include "Message.hpp"
#include <map>
#include <spdlog/spdlog.h>

using namespace Network::Messages;

BaseMessage::BaseMessage(const e_Type eType, const Address &sourceID, const Address &destinationID)
: m_eType(eType), m_sourceID(sourceID), m_destinationID(destinationID)
{
    // Nothing
}

const std::string & BaseMessage::typeToString() const
{
    return toString(m_eType);
}

size_t BaseMessage::size() const
{
    size_t size = 0;

    size += sizeof(m_eType);
    size += (m_sourceID.has_value() ? sizeof(m_sourceID.value()) : 1);
    size += (m_destinationID.has_value() ? sizeof(m_destinationID.value()) : 1);

    return size;
}

Acknowledge::Acknowledge(const size_t sourceID, const size_t destinationID, const e_Type ackType)
: BaseMessage(e_Type::Acknowledge, sourceID, destinationID), m_ackType(ackType)
{
    if(e_Type::Acknowledge == m_ackType) {
        throw std::invalid_argument("Acknowledge type cannot be Acknowledge");
    }
}

size_t Acknowledge::size() const
{
    const auto size = BaseMessage::size() + sizeof(m_ackType);

    return size;
}

DirectMessage::DirectMessage(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::DirectMessage, sourceID, destinationID), m_data()
{
    // Nothing
}

size_t DirectMessage::size() const
{
    const auto payloadSize = m_data.size() * sizeof(m_data.front());

    return (BaseMessage::size() + sizeof(m_data) + payloadSize);
}

BroadcastMessage::BroadcastMessage(const size_t sourceID)
: BaseMessage(e_Type::BroadcastMessage, sourceID)
{
    // Nothing
}

BroadcastMessage::BroadcastMessage(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::BroadcastMessage, sourceID, destinationID)
{
    // Nothing
}

size_t BroadcastMessage::size() const
{
    const auto size = BaseMessage::size() + sizeof(m_data) + (m_data.size() * sizeof(m_data.front()));

    return size;
}

BarrierRequest::BarrierRequest()
: BaseMessage(e_Type::BarrierRequest)
{
    // Nothing
}

BarrierRequest::BarrierRequest(const size_t sourceID)
: BaseMessage(e_Type::BarrierRequest, sourceID)
{
    // Nothing
}

BarrierRequest::BarrierRequest(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::BarrierRequest, sourceID, destinationID)
{
    // Nothing
}

size_t BarrierRequest::size() const
{
    return BaseMessage::size();
}

BarrierRelease::BarrierRelease()
: BaseMessage(e_Type::BarrierRelease)
{
    // Nothing
}

BarrierRelease::BarrierRelease(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::BarrierRelease, sourceID, destinationID)
{
    // Nothing
}

size_t BarrierRelease::size() const
{
    return BaseMessage::size();
}

Reduce::Reduce(const size_t destinationID, const OpType opType)
: BaseMessage(e_Type::Reduce, std::nullopt, destinationID), m_opType(opType)
{
    // Nothing
}

Reduce::Reduce(const size_t sourceID, const size_t destinationID, const OpType opType)
: BaseMessage(e_Type::Reduce, sourceID, destinationID), m_opType(opType)
{
    // Nothing
}

size_t Reduce::size() const
{
    const auto size = BaseMessage::size() + sizeof(m_opType) + sizeof(m_data) + (m_data.size() * sizeof(m_data.front()));

    return size;
}

ReduceAll::ReduceAll(const OpType opType)
: BaseMessage(e_Type::ReduceAll), m_opType(opType)
{
    // Nothing
}

ReduceAll::ReduceAll(const size_t sourceID, const size_t destinationID, const OpType opType)
: BaseMessage(e_Type::ReduceAll, sourceID, destinationID), m_opType(opType)
{
    // Nothing
}

size_t ReduceAll::size() const
{
    const auto size = BaseMessage::size() + sizeof(m_opType) + sizeof(m_data) + (m_data.size() * sizeof(m_data.front()));

    return size;
}

Scatter::Scatter(const size_t sourceID)
: BaseMessage(e_Type::Scatter, sourceID)
{
    // Nothing
}

Scatter::Scatter(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::Scatter, sourceID, destinationID)
{
    // Nothing
}

size_t Scatter::size() const
{
    const auto size = BaseMessage::size() + sizeof(m_data) + (m_data.size() * sizeof(m_data.front()));

    return size;
}

Gather::Gather(const size_t destinationID)
: BaseMessage(e_Type::Gather, std::nullopt, destinationID)
{
    // Nothing
}

Gather::Gather(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::Gather, sourceID, destinationID)
{
    // Nothing
}

size_t Gather::size() const
{
    const auto size = BaseMessage::size() + sizeof(m_data) + (m_data.size() * sizeof(m_data.front()));

    return size;
}

AllGather::AllGather()
: BaseMessage(e_Type::AllGather)
{
    // Nothing
}

AllGather::AllGather(const size_t sourceID, const size_t destinationID)
: BaseMessage(e_Type::AllGather, sourceID, destinationID)
{
    // Nothing
}

size_t AllGather::size() const
{
    const auto size = BaseMessage::size() + sizeof(m_data) + (m_data.size() * sizeof(m_data.front()));

    return size;
}

const std::string &Network::Messages::toString(const e_Type eType)
{
    static const std::map<e_Type, std::string> strMap {
        {e_Type::Acknowledge,      "Acknowledge"},
        {e_Type::DirectMessage,    "DirectMessage"},
        {e_Type::BroadcastMessage, "BroadcastMessage"},
        {e_Type::BarrierRequest,   "BarrierRequest"},
        {e_Type::BarrierRelease,   "BarrierRelease"},
        {e_Type::Reduce,           "Reduce"},
        {e_Type::ReduceAll,        "ReduceAll"},
        {e_Type::Scatter,          "Scatter"},
        {e_Type::Gather,           "Gather"},
        {e_Type::AllGather,        "AllGather"},

        {e_Type::IS_Reduce,        "Inter-Switch Reduce"},
        {e_Type::IS_Scatter,       "Inter-Switch Scatter"},
        {e_Type::IS_Gather,        "Inter-Switch Gather"},
        {e_Type::IS_AllGather,     "Inter-Switch AllGather"},
    };

    return strMap.at(eType);
}

const std::string &Network::Messages::toString(const ReduceOperation opType)
{
    static const std::map<ReduceOperation, std::string> strMap {
        {ReduceOperation::Sum,      "Sum"},
        {ReduceOperation::Multiply, "Multiply"},
        {ReduceOperation::Max,      "Max"},
        {ReduceOperation::Min,      "Min"},
    };

    return strMap.at(opType);
}
