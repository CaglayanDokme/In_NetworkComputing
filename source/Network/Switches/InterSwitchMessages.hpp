#pragma once

#include "Network/Message.hpp"

#include <string>
#include <vector>

namespace Network::Messages::InterSwitch {
class Reduce : public BaseMessage {
public: /** Construction **/
    Reduce() = delete;
    explicit Reduce(const size_t destinationID);

public: /** Methods **/
    /**
         * @brief Get the size of the message in bytes
         * @return The size of the message in bytes
         */
    [[nodiscard]] size_t size() const final;

public:                                 /** Data **/
    std::vector<size_t> m_contributors; // Computing node IDs that contributed to the reduction
    std::vector<float>  m_data;
    ReduceOperation     m_opType;
};

class Scatter : public BaseMessage {
public: /** Construction **/
    Scatter() = delete;
    explicit Scatter(const size_t sourceID);

public: /** Methods **/
    /**
         * @brief Get the size of the message in bytes
         * @return The size of the message in bytes
         */
    [[nodiscard]] size_t size() const final;

public: /** Data **/
    // clang-format off
    std::vector<
        std::pair<
            size_t,            // Destination computing node ID
            std::vector<float> // Data to be scattered
        >
    > m_data;
    // clang-format on
};

class Gather : public BaseMessage {
public: /** Construction **/
    Gather() = delete;
    explicit Gather(const size_t destinationID);

public: /** Methods **/
    /**
         * @brief Get the size of the message in bytes
         * @return The size of the message in bytes
         */
    [[nodiscard]] size_t size() const final;

public: /** Data **/
    // clang-format off
    std::vector<
        std::pair<
            size_t,            // Source computing node ID
            std::vector<float> // Data to be gathered
        >
    > m_data;
    // clang-format on
};

class AllGather : public BaseMessage {
public: /** Construction **/
    AllGather();

public: /** Methods **/
    /**
         * @brief Get the size of the message in bytes
         * @return The size of the message in bytes
         */
    [[nodiscard]] size_t size() const final;

public: /** Data **/
    // clang-format off
    std::vector<
        std::pair<
            size_t,            // Source computing node ID
            std::vector<float> // Data to be gathered
        >
    > m_data;
    // clang-format on
};
}; // namespace Network::Messages::InterSwitch
