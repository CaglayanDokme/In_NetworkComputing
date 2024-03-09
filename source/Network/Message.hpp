#pragma once

#include <cstddef>
#include <string>

namespace Network::Messages {
    enum class e_Type {
        Message,
        BroadcastMessage,
        BarrierRequest,
        BarrierRelease,
        Reduce,
        ReduceAll
    };

    [[nodiscard]] std::string toString(const e_Type eType);

    class BaseMessage {
    protected: /** Construction **/
        BaseMessage() = delete;
        BaseMessage(const e_Type eType);

    public: /** Methods **/
        [[nodiscard]] e_Type type() const { return m_eType; }

        [[nodiscard]] std::string typeToString() const;

    protected:
        const e_Type m_eType;
    };

    class DirectMessage : public BaseMessage {
    public: /** Construction **/
        DirectMessage() = delete;
        explicit DirectMessage(const std::size_t sourceID, const std::size_t destinationID);

    public: /** Addressing **/
        const std::size_t m_sourceID;
        const std::size_t m_destinationID;

    public: /** Data **/
        float m_data;
    };

    class BroadcastMessage : public BaseMessage {
    public: /** Construction **/
        BroadcastMessage() = delete;
        explicit BroadcastMessage(const std::size_t sourceID);

    public: /** Addressing **/
        const std::size_t m_sourceID;

    public: /** Data **/
        float data;
    };

    class BarrierRequest : public BaseMessage {
    public: /** Construction **/
        BarrierRequest() = delete;
        explicit BarrierRequest(const std::size_t sourceID);

    public: /** Addressing **/
        const std::size_t m_sourceID;
    };

    class BarrierRelease : public BaseMessage {
    public: /** Construction **/
        BarrierRelease();

        // This message doesn't require any addressing or data
    };

    class Reduce : public BaseMessage {
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

    class ReduceAll : public BaseMessage {
    public: /** Enumerations **/
        enum class OpType {
            Sum,
            Multiply,
            Max,
            Min
        };

    public: /** Construction **/
        ReduceAll() = delete;
        explicit ReduceAll(const OpType opType);

    public: /** Data **/
        const OpType m_opType;
        float m_data;
    };
};
