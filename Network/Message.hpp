#pragma once

#include <cstddef>

namespace Network {
    // TODO namespace Messages

    class Message {
    public: /** Construction **/
        Message() = delete;
        explicit Message(const std::size_t sourceID, const std::size_t destinationID);

    public: /** Addressing **/
        const std::size_t m_sourceID;
        const std::size_t m_destinationID;

    public: /** Data **/
        struct {
            float data;
        } m_data;
    };

    class BroadcastMessage {
    public: /** Construction **/
        BroadcastMessage() = delete;
        explicit BroadcastMessage(const std::size_t sourceID);

    public: /** Addressing **/
        const std::size_t m_sourceID;

    public: /** Data **/
        struct {
            float data;
        } m_data;
    };

    class BarrierRequest {
    public: /** Construction **/
        BarrierRequest() = delete;
        explicit BarrierRequest(const std::size_t sourceID);

    public: /** Addressing **/
        const std::size_t m_sourceID;
    };

    class BarrierRelease {
    public: /** Construction **/
        BarrierRelease() = default;

        // This message doesn't require any addressing or data
    };
};
