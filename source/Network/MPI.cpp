/**
 * @file    MPI.hpp
 * @brief   Implementation of the MPI class which stands for the computing and message passing interface of the network.
 * @author  Caglayan Dokme (caglayan.dokme@plan.space)
 * @date    March 17, 2024
 */

#include "MPI.hpp"

#include "spdlog/spdlog.h"
#include "Derivations.hpp"

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
        case State::Acknowledge: {
            if(anyMsg->type() != Messages::e_Type::Acknowledge) {
                spdlog::critical("MPI({}): Received a message of type {} while in acknowledge state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            const auto &msg = *static_cast<const Messages::Acknowledge *>(anyMsg.release());

            {
                std::lock_guard lock(m_acknowledge.mutex);
                spdlog::trace("MPI({}): Saving acknowledgement from {}", m_ID, msg.m_sourceID);

                m_acknowledge.sourceID = msg.m_sourceID;
                m_acknowledge.ackType = msg.m_ackType;
            }

            setState(State::Idle);
            m_acknowledge.notifier.notify_one();

            break;
        }
        case State::Receive: {
            if(anyMsg->type() != Messages::e_Type::DirectMessage) {
                spdlog::critical("MPI({}): Received a message of type {} while in receive state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            const auto &msg = *static_cast<const Messages::DirectMessage *>(anyMsg.release());

            if(msg.m_destinationID != m_ID) {
                spdlog::critical("MPI({}): Received a message for another node({}) while in receive state!", m_ID, msg.m_destinationID);

                throw std::logic_error("MPI cannot receive a message for another node!");
            }

            if(msg.m_data.empty()) {
                spdlog::critical("MPI({}): Empty data received from node #{}!", m_ID, msg.m_sourceID);

                throw std::runtime_error("MPI mustn't received empty message!");
            }

            {
                std::lock_guard lock(m_directReceive.mutex);

                m_directReceive.receivedData = std::move(msg.m_data);
                m_directReceive.sourceID = msg.m_sourceID;
            }

            setState(State::Idle);
            m_directReceive.notifier.notify_one();

            break;
        }
        case State::BroadcastReceive: {
            if(anyMsg->type() != Messages::e_Type::BroadcastMessage) {
                spdlog::critical("MPI({}): Received a message of type {} while in broadcast receive state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            const auto &msg = *static_cast<const Messages::BroadcastMessage *>(anyMsg.release());

            {
                std::lock_guard lock(m_broadcastReceive.mutex);

                m_broadcastReceive.receivedData = std::move(msg.m_data);
                m_broadcastReceive.sourceID = msg.m_sourceID;
            }


            setState(State::Idle);
            m_broadcastReceive.notifier.notify_one();

            break;
        }
        case State::Barrier: {
            if(anyMsg->type() != Messages::e_Type::BarrierRelease) {
                spdlog::critical("MPI({}): Received a message of type {} while in barrier state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            setState(State::Idle);
            m_barrier.notifier.notify_one();

            break;
        }
        case State::Reduce: {
            if(anyMsg->type() != Messages::e_Type::Reduce) {
                spdlog::critical("MPI({}): Received a message of type {} while in reduce state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            const auto &msg = *static_cast<const Messages::Reduce *>(anyMsg.release());

            {
                std::lock_guard lock(m_reduce.mutex);

                m_reduce.receivedData = msg.m_data;
                m_reduce.operation = msg.m_opType;
            }

            setState(State::Idle);
            m_reduce.notifier.notify_one();

            break;
        }
        case State::ReduceAll: {
            if(anyMsg->type() != Messages::e_Type::ReduceAll) {
                spdlog::critical("MPI({}): Received a message of type {} while in reduce-all state!", m_ID, anyMsg->typeToString());

                throw std::logic_error("MPI cannot receive a message of this type!");
            }

            const auto &msg = *static_cast<const Messages::ReduceAll *>(anyMsg.release());

            {
                std::lock_guard lock(m_reduceAll.mutex);

                m_reduceAll.receivedData = msg.m_data;
                m_reduceAll.operation = msg.m_opType;
            }

            setState(State::Idle);
            m_reduceAll.notifier.notify_one();

            break;
        }
        default: {
            spdlog::critical("MPI({}): Invalid state({})!", m_ID, static_cast<int>(m_state));
            throw std::logic_error("Invalid MPI state!");
        }
    }
}

bool MPI::isReady() const
{
    return m_port.isConnected();
}

void MPI::send(const std::vector<float> &data, const size_t destinationID)
{
    spdlog::trace("MPI({}): Sending data to {}", m_ID, destinationID);

    if(data.empty()) {
        spdlog::critical("MPI({}): Cannot send an empty message!", m_ID);

        throw std::invalid_argument("MPI cannot send empty message!");
    }

    // Send direct message
    {
        // Create a message
        auto msg = std::make_unique<Messages::DirectMessage>(m_ID, destinationID);

        msg->m_data = data;

        // Push the message to the port
        m_port.pushOutgoing(std::move(msg));
    }

    // Wait for an acknowledgement
    {
        setState(State::Acknowledge);

        std::unique_lock lock(m_acknowledge.mutex);
        m_acknowledge.notifier.wait(lock);

        if(m_acknowledge.sourceID != destinationID) {
            spdlog::critical("MPI({}): Received acknowledgement from invalid source({}), expected #{}", m_ID, m_acknowledge.sourceID, destinationID);

            throw std::logic_error("MPI: Invalid source ID!");
        }

        if(Messages::e_Type::DirectMessage != m_acknowledge.ackType) {
            spdlog::critical("MPI({}): Received acknowledgement of invalid type({})!", m_ID, Messages::toString(m_acknowledge.ackType));

            throw std::logic_error("MPI: Invalid acknowledgement type!");
        }
    }
}

void MPI::receive(std::vector<float> &data, const size_t sourceID)
{
    spdlog::trace("MPI({}): Receiving data from {}", m_ID, sourceID);

    setState(State::Receive);


    if(!data.empty()) {
        spdlog::critical("MPI({}): Cannot receive into a non-empty destination!", m_ID);
        spdlog::debug("MPI({}): Destination had {} elements", m_ID, data.size());

        throw std::invalid_argument("Receive destination must be empty!");
    }

    // Wait for a message
    {
        std::unique_lock lock(m_directReceive.mutex);
        m_directReceive.notifier.wait(lock);

        if(m_directReceive.sourceID != sourceID) {
            spdlog::critical("MPI({}): Received data from invalid source({}), expected {}!", m_ID, m_directReceive.sourceID, sourceID);

            throw std::logic_error("MPI: Invalid source ID!");
        }

        data = std::move(m_directReceive.receivedData);
    }

    // Send an acknowledgement
    {
        auto msg = std::make_unique<Messages::Acknowledge>(m_ID, sourceID, Messages::e_Type::DirectMessage);
        m_port.pushOutgoing(std::move(msg));

        spdlog::trace("MPI({}): Sent acknowledgement to {}", m_ID, sourceID);
    }
}

void MPI::broadcast(std::vector<float> &data, const size_t sourceID)
{
    if(m_ID == sourceID) {
        spdlog::trace("MPI({}): Broadcasting..", m_ID);

        if(data.empty()) {
            spdlog::critical("MPI({}): Cannot send an empty message!", m_ID);

            throw std::invalid_argument("MPI cannot send empty message!");
        }


        // Broadcast message
        {
            auto msg = std::make_unique<Messages::BroadcastMessage>(m_ID);
            msg->m_data = data;

            m_port.pushOutgoing(std::move(msg));
        }

        // Wait for acknowledgements
        {
            const auto compNodeAmount = Network::Utilities::deriveComputingNodeAmount();
            std::vector<bool> acks(compNodeAmount, false);
            acks.at(m_ID) = true; // Skip the broadcaster

            while(true) {
                setState(State::Acknowledge);

                // Wait for notification
                std::unique_lock lock(m_acknowledge.mutex);
                m_acknowledge.notifier.wait(lock);

                if(m_acknowledge.ackType != Messages::e_Type::BroadcastMessage) {
                    spdlog::critical("MPI({}): Received acknowledgement of invalid type({})!", m_ID, Messages::toString(m_acknowledge.ackType));

                    throw std::logic_error("MPI: Invalid acknowledgement type!");
                }

                if(acks[m_acknowledge.sourceID]) {
                    spdlog::critical("MPI({}): Received duplicate acknowledgement from {}", m_ID, m_acknowledge.sourceID);

                    throw std::logic_error("MPI: Duplicate acknowledgement!");
                }

                acks.at(m_acknowledge.sourceID) = true;
                spdlog::trace("MPI({}): Received acknowledgement from {}", m_ID, m_acknowledge.sourceID);

                if(std::all_of(acks.cbegin(), acks.cend(), [](const bool ack) { return ack; })) {
                    spdlog::trace("MPI({}): All acknowledgements received", m_ID);

                    break;
                }
            }
        }
    }
    else {
        setState(State::BroadcastReceive);

        if(!data.empty()) {
            spdlog::critical("MPI({}): Cannot receive into a non-empty destination!", m_ID);
            spdlog::debug("MPI({}): Destination had {} elements", m_ID, data.size());

            throw std::invalid_argument("Receive destination must be empty!");
        }

        // Wait for broadcast message
        {
            spdlog::trace("MPI({}): Receiving broadcast from {}", m_ID, sourceID);
            std::unique_lock lock(m_broadcastReceive.mutex);
            m_broadcastReceive.notifier.wait(lock);

            if(m_broadcastReceive.sourceID != sourceID) {
                spdlog::critical("MPI({}): Received data from invalid source({}), expected {}!", m_ID, m_broadcastReceive.sourceID, sourceID);

                throw std::logic_error("MPI: Invalid source ID!");
            }

            data = std::move(m_broadcastReceive.receivedData);
        }

        // Send an acknowledgement
        {
            auto msg = std::make_unique<Messages::Acknowledge>(m_ID, sourceID, Messages::e_Type::BroadcastMessage);
            m_port.pushOutgoing(std::move(msg));

            spdlog::trace("MPI({}): Sent acknowledgement to {}", m_ID, sourceID);
        }
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
            spdlog::critical("MPI({}): Received data with invalid operation({})! Expected {}", m_ID, Messages::toString(m_reduce.operation), Messages::toString(operation));

            throw std::logic_error("MPI: Invalid operation!");
        }

        data = Messages::reduce(data, m_reduce.receivedData, operation);
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
        spdlog::critical("MPI({}): Received data with invalid reduce-all operation({})! Expected {}", m_ID, Messages::toString(m_reduceAll.operation), Messages::toString(operation));

        throw std::logic_error("MPI: Invalid reduce-all operation!");
    }

    data = m_reduceAll.receivedData;
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
