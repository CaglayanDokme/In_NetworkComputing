#pragma once

#include <cstddef>

namespace Network::Messages {
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

    class Reduce {
    public: /** Enumerations **/
        enum class OpType {
            Sum,
            Multiply,
            Max,
            Min
        };

    public: /** Construction **/
        Reduce() = delete;
        explicit Reduce(const std::size_t destinationID, const OpType opType);

    public: /** Addressing **/
        const std::size_t m_destinationID;

    public: /** Data **/
        const OpType m_opType;
        float m_data;
    };
};
