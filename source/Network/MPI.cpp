/**
 * @file    MPI.hpp
 * @brief   Implementation of the MPI class which stands for the computing and message passing interface of the network.
 * @author  Caglayan Dokme (caglayan.dokme@plan.space)
 * @date    March 17, 2024
 */

#include "MPI.hpp"

#include "spdlog/spdlog.h"

using namespace Network;

MPI::MPI(const std::size_t ID)
: m_ID(ID)
{
    spdlog::trace("MPI({}): Created", m_ID);
}

void MPI::tick()
{
    // Advance port
    m_port.tick();

    if(!m_port.hasIncoming()) {
        return;
    }
    else if(State::Idle == m_state) {
        return;
    }
    else {
        // Nothing to do
    }

    auto anyMsg = m_port.popIncoming();
    spdlog::trace("MPI({}): Received message type {}", m_ID, anyMsg->typeToString());

    switch(m_state) {
        case State::Receive: {
            if(anyMsg->type() != Messages::e_Type::Message) {
                spdlog::critical("MPI({}): Received a message of type {} while in receive state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            const auto &msg = *static_cast<const Messages::DirectMessage *>(anyMsg.release());

            if(msg.m_destinationID != m_ID) {
                spdlog::critical("MPI({}): Received a message for another node({}) while in receive state!", m_ID, msg.m_destinationID);

                throw std::logic_error("MPI cannot receive a message for another node!");
            }

            m_directReceive.receivedData = msg.m_data;
            m_directReceive.sourceID = msg.m_sourceID;

            m_directReceive.notifier.notify_one();

            break;
        }
        case State::BroadcastReceive: {
            if(anyMsg->type() != Messages::e_Type::BroadcastMessage) {
                spdlog::critical("MPI({}): Received a message of type {} while in broadcast receive state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            const auto &msg = *static_cast<const Messages::BroadcastMessage *>(anyMsg.release());

            m_broadcastReceive.receivedData = msg.data;
            m_broadcastReceive.sourceID = msg.m_sourceID;

            m_broadcastReceive.notifier.notify_one();

            break;
        }
        case State::Barrier: {
            if(anyMsg->type() != Messages::e_Type::BarrierRelease) {
                spdlog::critical("MPI({}): Received a message of type {} while in barrier state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            m_barrier.notifier.notify_one();

            break;
        }
        case State::Reduce: {
            if(anyMsg->type() != Messages::e_Type::Reduce) {
                spdlog::critical("MPI({}): Received a message of type {} while in reduce state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            const auto &msg = *static_cast<const Messages::Reduce *>(anyMsg.release());

            m_reduce.receivedData = msg.m_data;
            m_reduce.operation = msg.m_opType;

            m_reduce.notifier.notify_one();

            break;
        }
        case State::ReduceAll: {
            if(anyMsg->type() != Messages::e_Type::ReduceAll) {
                spdlog::critical("MPI({}): Received a message of type {} while in reduce-all state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            const auto &msg = *static_cast<const Messages::ReduceAll *>(anyMsg.release());

            m_reduceAll.receivedData = msg.m_data;
            m_reduceAll.operation = msg.m_opType;

            m_reduceAll.notifier.notify_one();

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
    spdlog::trace("MPI({}): Sending data {} to {}", m_ID, data, destinationID);

    // Create a message
    auto msg = std::make_unique<Messages::DirectMessage>(m_ID, destinationID);

    msg->m_data = data;

    // Push the message to the port
    m_port.pushOutgoing(std::move(msg));
}

void MPI::receive(float &data, const size_t sourceID)
{
    spdlog::trace("MPI({}): Receiving data from {}", m_ID, sourceID);
    setState(State::Receive);

    std::unique_lock lock(m_directReceive.mutex);
    m_directReceive.notifier.wait(lock);

    if(m_directReceive.sourceID != sourceID) {
        spdlog::critical("MPI({}): Received data from invalid source({})!", m_ID, m_directReceive.sourceID);

        throw std::logic_error("MPI: Invalid source ID!");
    }

    data = m_directReceive.receivedData;

    setState(State::Idle);
}

void MPI::broadcast(float &data, const size_t sourceID)
{
    if(m_ID == sourceID) {
        spdlog::trace("MPI({}): Broadcasting data {}", m_ID, data);

        // Create a message
        auto msg = std::make_unique<Messages::BroadcastMessage>(m_ID);

        msg->data = data;

        // Push the message to the port
        m_port.pushOutgoing(std::move(msg));
    }
    else {
        spdlog::trace("MPI({}): Receiving broadcast from {}", m_ID, sourceID);
        setState(State::BroadcastReceive);

        std::unique_lock lock(m_broadcastReceive.mutex);
        m_broadcastReceive.notifier.wait(lock);

        if(m_broadcastReceive.sourceID != sourceID) {
            spdlog::critical("MPI({}): Received data from invalid source({}), expected {}!", m_ID, m_broadcastReceive.sourceID, sourceID);

            throw std::logic_error("MPI: Invalid source ID!");
        }

        data = m_broadcastReceive.receivedData;

        setState(State::Idle);
    }
}

void MPI::barrier()
{
    spdlog::trace("MPI({}): Barrier", m_ID);
    setState(State::Barrier);

    // Create a message
    auto msg = std::make_unique<Messages::BarrierRequest>(m_ID);

    // Push the message to the port
    m_port.pushOutgoing(std::move(msg));

    // Wait for the barrier to be released
    std::unique_lock lock(m_barrier.mutex);
    m_barrier.notifier.wait(lock);

    setState(State::Idle);

    spdlog::trace("MPI({}): Barrier released", m_ID);
}

void MPI::reduce(float &data, const ReduceOp operation, const size_t destinationID)
{
    spdlog::trace("MPI({}): Reducing data at {}", m_ID, destinationID);

    if(m_ID == destinationID) {
        setState(State::Reduce);

        std::unique_lock lock(m_reduce.mutex);
        m_reduce.notifier.wait(lock);

        if(m_reduce.operation != operation) {
            spdlog::critical("MPI({}): Received data with invalid operation({})! Expected {}", m_ID, static_cast<int>(m_reduce.operation), static_cast<int>(operation));

            throw std::logic_error("MPI: Invalid operation!");
        }

        data = Messages::reduce(data, m_reduce.receivedData, operation);

        setState(State::Idle);
    }
    else {
        auto msg = std::make_unique<Messages::Reduce>(destinationID, operation);
        msg->m_data = data;

        // Push the message to the port
        m_port.pushOutgoing(std::move(msg));
    }
}

void MPI::reduceAll(float &data, const ReduceOp operation)
{
    spdlog::trace("MPI({}): Reducing data", m_ID);

    setState(State::ReduceAll);

    auto msg = std::make_unique<Messages::ReduceAll>(operation);
    msg->m_data = data;

    m_port.pushOutgoing(std::move(msg));

    std::unique_lock lock(m_reduceAll.mutex);
    m_reduceAll.notifier.wait(lock);

    if(m_reduceAll.operation != operation) {
        spdlog::critical("MPI({}): Received data with invalid reduce-all operation({})! Expected {}", m_ID, static_cast<int>(m_reduceAll.operation), static_cast<int>(operation));

        throw std::logic_error("MPI: Invalid reduce-all operation!");
    }

    data = m_reduceAll.receivedData;

    setState(State::Idle);
}

void MPI::setState(const State state)
{
    if(State::Idle == state) {
        m_state = state;

        return;
    }

    if(State::Idle != m_state) {
        spdlog::critical("MPI({}): Invalid state transition from {} to {}!", m_ID, static_cast<int>(m_state), static_cast<int>(state));

        throw std::logic_error("Invalid MPI state transition!");
    }

    m_state = state;
}
