#pragma once

#include <cstddef>
#include <string>
#include <vector>
#include <stdexcept>

namespace Network::Messages {
    enum class e_Type {
        Acknowledge,
        DirectMessage,
        BroadcastMessage,
        BarrierRequest,
        BarrierRelease,
        Reduce,
        ReduceAll
    };

    enum class ReduceOperation {
        Sum,
        Multiply,
        Max,
        Min
    };

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

    class Acknowledge : public BaseMessage {
    public: /** Construction **/
        Acknowledge() = delete;
        explicit Acknowledge(const std::size_t sourceID, const std::size_t destinationID, const e_Type ackType);

    public: /** Addressing **/
        const std::size_t m_sourceID;
        const std::size_t m_destinationID;
        const e_Type m_ackType;
    };

    class DirectMessage : public BaseMessage {
    public: /** Construction **/
        DirectMessage() = delete;
        explicit DirectMessage(const std::size_t sourceID, const std::size_t destinationID);

    public: /** Addressing **/
        const std::size_t m_sourceID;
        const std::size_t m_destinationID;

    public: /** Data **/
        std::vector<float> m_data;
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
        using OpType = ReduceOperation;

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
        using OpType = ReduceOperation;

    public: /** Construction **/
        ReduceAll() = delete;
        explicit ReduceAll(const OpType opType);

    public: /** Data **/
        const OpType m_opType;
        float m_data;
    };

    /*** Helper Methods ***/
    /**
     * @brief  Convert the message type to a string
     * @param  eType Message type
     * @return String representation of the message type
     */
    [[nodiscard]] const std::string &toString(const e_Type eType);

    /**
     * @brief  Convert the reduce operation to a string
     * @param  opType Reduce operation
     * @return String representation of the reduce operation
     */
    [[nodiscard]] const std::string &toString(const ReduceOperation opType);

    /**
     * @brief  Reduce two values using the given operation
     * @tparam T Type of the values
     * @param  a First value
     * @param  b Second value
     * @param  operation Operation to perform
     * @return Reduced value
     */
    template<class T>
    [[nodiscard]] T reduce(const T &a, const T &b, const ReduceOperation operation)
    {
        switch(operation) {
            case ReduceOperation::Sum:
                return a + b;
            case ReduceOperation::Multiply:
                return a * b;
            case ReduceOperation::Max:
                return (a > b) ? a : b;
            case ReduceOperation::Min:
                return (a < b) ? a : b;
            default:
                throw std::logic_error("Unknown reduce operation!");
        }
    }
};
