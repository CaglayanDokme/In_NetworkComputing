#include "MPI.hpp"

#include "spdlog/spdlog.h"
#include "Message.hpp"

using namespace Network;

MPI::MPI(const std::size_t ID)
: m_ID(ID)
{
    spdlog::trace("MPI({}: Created", m_ID);
}

void MPI::tick()
{
    // Advance port
    m_port.tick();

    if(!m_port.hasIncoming()) {
        return;
    }

    auto anyMsg = m_port.popIncoming();
    spdlog::trace("MPI({}): Received message type {}", m_ID, anyMsg->typeToString());

    if(State::Idle == m_state) {
        spdlog::critical("MPI({}): Although in idle state, node received a message!", m_ID);

        throw std::logic_error("MPI cannot receive in idle state!");
    }

    switch(m_state) {
        case State::Receive: {
            if(anyMsg->type() != Messages::e_Type::Message) {
                spdlog::critical("MPI({}): Received a message of type {} while in receive state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            const auto &msg = *static_cast<const Messages::DirectMessage *>(anyMsg.get());

            m_directReceive.receivedData = msg.m_data;
            m_directReceive.sourceID = msg.m_sourceID;

            m_directReceive.notifier.notify_one();

            break;
        }
        default:
            spdlog::critical("MPI({}): Invalid state({})!", m_ID, static_cast<int>(m_state));
            throw std::logic_error("Invalid MPI state!");
    }
}

bool MPI::isReady() const
{
    return m_port.isConnected();
}

void MPI::send(const float &data, const size_t destinationID)
{
    // Create a message
    auto msg = std::make_unique<Messages::DirectMessage>(m_ID, destinationID);

    msg->m_data = data;

    // Push the message to the port
    m_port.pushOutgoing(std::move(msg));
}

void MPI::receive(float &data, const size_t sourceID)
{
    setState(State::Receive);

    std::unique_lock lock(m_directReceive.mutex);
    m_directReceive.notifier.wait(lock);

    if(m_directReceive.sourceID != sourceID) {
        spdlog::critical("MPI({}): Received data from invalid source({})!", m_ID, m_directReceive.sourceID);

        throw std::logic_error("MPI: Invalid source ID!");
    }

    data = m_directReceive.receivedData;
}

void MPI::setState(const State state)
{
    if(State::Idle == m_state) {
        m_state = state;

        return;
    }

    if(State::Idle != m_state) {
        spdlog::critical("MPI({}): Invalid state transition from {} to {}!", m_ID, static_cast<int>(m_state), static_cast<int>(state));

        throw std::logic_error("Invalid MPI state transition!");
    }

    m_state = state;
}
