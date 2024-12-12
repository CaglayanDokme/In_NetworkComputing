#pragma once

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace Network::Messages {
enum class e_Type {
    Acknowledge,
    DirectMessage,
    BroadcastMessage,
    BarrierRequest,
    BarrierRelease,
    Reduce,
    ReduceAll,
    Scatter,
    Gather,
    AllGather,

    // Inter-switch messages
    IS_Reduce,
    IS_Scatter,
    IS_Gather,
    IS_AllGather,
};

enum class ReduceOperation {
    Sum,
    Multiply,
    Max,
    Min
};

class BaseMessage {
public: /** Aliases **/
    using Address = std::optional<size_t>;

protected: /** Construction **/
    BaseMessage() = delete;
    BaseMessage(const e_Type eType, const Address &sourceID = std::nullopt, const Address &destinationID = std::nullopt);

public: /** Methods **/
    [[nodiscard]] e_Type type() const { return m_eType; }

    [[nodiscard]] const std::string &typeToString() const;

    [[nodiscard]] virtual size_t size() const;

public: /** Members **/
    const Address m_sourceID;
    const Address m_destinationID;
    const e_Type  m_eType;
};

class Acknowledge : public BaseMessage {
public: /** Construction **/
    Acknowledge() = delete;
    explicit Acknowledge(const size_t sourceID, const size_t destinationID, const e_Type ackType);

public: /** Methods **/
    /**
     * @brief Get the size of the message in bytes
     * @return The size of the message in bytes
     */
    [[nodiscard]] size_t size() const final;

public: /** Addressing **/
    const e_Type m_ackType;
};

class DirectMessage : public BaseMessage {
public: /** Construction **/
    DirectMessage() = delete;
    explicit DirectMessage(const size_t sourceID, const size_t destinationID);

public: /** Methods **/
    /**
     * @brief Get the size of the message in bytes
     * @return The size of the message in bytes
     */
    [[nodiscard]] size_t size() const final;

public: /** Data **/
    std::vector<float> m_data;
};

class BroadcastMessage : public BaseMessage {
public: /** Construction **/
    BroadcastMessage() = delete;

    /**
     * @brief Construct a message to be broadcasted by in-network computing capable switches
     * @param sourceID ID of the source computing node
     */
    explicit BroadcastMessage(const size_t sourceID);

    /**
     * @brief Construct a broadcast message to just be redirected by switches
     * @param sourceID      ID of the source computing node
     * @param destinationID ID of the destination computing node
     */
    explicit BroadcastMessage(const size_t sourceID, const size_t destinationID);

public: /** Methods **/
    /**
     * @brief Get the size of the message in bytes
     * @return The size of the message in bytes
     */
    [[nodiscard]] size_t size() const final;

public: /** Data **/
    std::vector<float> m_data;
};

class BarrierRequest : public BaseMessage {
public: /** Construction **/
    BarrierRequest();

    /**
     * @brief Construct a barrier request message that will be responded by in-network computing capable switches
     * @param sourceID ID of the source computing node
     */
    explicit BarrierRequest(const size_t sourceID);

    /**
     * @brief Construct a barrier request message that will just be redirected by switches
     * @param sourceID      ID of the source computing node
     * @param destinationID ID of the destination computing node
     */
    explicit BarrierRequest(const size_t sourceID, const size_t destinationID);

public: /** Methods **/
    /**
     * @brief Get the size of the message in bytes
     * @return The size of the message in bytes
     */
    [[nodiscard]] size_t size() const final;
};

class BarrierRelease : public BaseMessage {
public: /** Construction **/
    /**
     * @brief Construct a barrier release message that will be broadcasted by in-network computing capable switches
     */
    BarrierRelease();

    /**
     * @brief Construct a barrier release message that will just be redirected by switches
     * @param sourceID      ID of the source computing node
     * @param destinationID ID of the destination computing node
     */
    BarrierRelease(const size_t sourceID, const size_t destinationID);

public: /** Methods **/
    /**
     * @brief Get the size of the message in bytes
     * @return The size of the message in bytes
     */
    [[nodiscard]] size_t size() const final;
};

class Reduce : public BaseMessage {
public: /** Enumerations **/
    using OpType = ReduceOperation;

public: /** Construction **/
    Reduce() = delete;

    /**
     * @brief Construct a reduce message that will be processed by in-network computing capable switches
     * @param destinationID ID of the destination computing node
     * @param opType        Operation to perform
     */
    explicit Reduce(const size_t destinationID, const OpType opType);

    /**
     * @brief Construct a reduce message that will just be redirected by switches
     * @param sourceID      ID of the source computing node
     * @param destinationID ID of the destination computing node
     * @param opType        Operation to perform
     */
    explicit Reduce(const size_t sourceID, const size_t destinationID, const OpType opType);

public: /** Methods **/
    /**
     * @brief Get the size of the message in bytes
     * @return The size of the message in bytes
     */
    [[nodiscard]] size_t size() const final;

public: /** Data **/
    const OpType       m_opType;
    std::vector<float> m_data;
};

class ReduceAll : public BaseMessage {
public: /** Enumerations **/
    using OpType = ReduceOperation;

public: /** Construction **/
    ReduceAll() = delete;

    /**
     * @brief Construct a reduce all message that will be processed by in-network computing capable switches
     * @param opType Operation to perform
     */
    explicit ReduceAll(const OpType opType);

    /**
     * @brief Construct a reduce all message that will just be redirected by switches
     * @param sourceID      ID of the source computing node
     * @param destinationID ID of the destination computing node
     * @param opType        Operation to perform
     */
    explicit ReduceAll(const size_t sourceID, const size_t destinationID, const OpType opType);

public: /** Methods **/
    /**
     * @brief Get the size of the message in bytes
     * @return The size of the message in bytes
     */
    [[nodiscard]] size_t size() const final;

public: /** Data **/
    const OpType       m_opType;
    std::vector<float> m_data;
};

class Scatter : public BaseMessage {
public: /** Construction **/
    Scatter() = delete;

    /**
     * @brief Construct a scatter message that will be processed by in-network computing capable switches
     * @param sourceID ID of the source computing node
     */
    explicit Scatter(const size_t sourceID);

    /**
     * @brief Construct a scatter message that will just be redirected by switches
     * @param sourceID      ID of the source computing node
     * @param destinationID ID of the destination computing node
     */
    explicit Scatter(const size_t sourceID, const size_t destinationID);

public: /** Methods **/
    /**
     * @brief Get the size of the message in bytes
     * @return The size of the message in bytes
     */
    [[nodiscard]] size_t size() const final;

public: /** Data **/
    std::vector<float> m_data;
};

class Gather : public BaseMessage {
public: /** Construction **/
    Gather() = delete;

    /**
     * @brief Construct a gather message that will be processed by in-network computing capable switches
     * @param destinationID ID of the destination computing node
     */
    explicit Gather(const size_t destinationID);

    /**
     * @brief Construct a gather message that will just be redirected by switches
     * @param sourceID      ID of the source computing node
     * @param destinationID ID of the destination computing node
     */
    explicit Gather(const size_t sourceID, const size_t destinationID);

public: /** Methods **/
    /**
     * @brief Get the size of the message in bytes
     * @return The size of the message in bytes
     */
    [[nodiscard]] size_t size() const final;

public: /** Data **/
    std::vector<float> m_data;
};

class AllGather : public BaseMessage {
public: /** Construction **/
    AllGather();

    /**
     * @brief Construct an all-gather message that will just be redirected by switches
     * @param sourceID      ID of the source computing node
     * @param destinationID ID of the destination computing node
     */
    explicit AllGather(const size_t sourceID, const size_t destinationID);

public: /** Methods **/
    /**
     * @brief Get the size of the message in bytes
     * @return The size of the message in bytes
     */
    [[nodiscard]] size_t size() const final;

public: /** Data **/
    std::vector<float> m_data;
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
}; // namespace Network::Messages
