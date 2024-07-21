#pragma once

#include "Network/Message.hpp"
#include <string>

namespace Network::Messages::InterSwitch {
    class Scatter : public BaseMessage {
    public: /** Construction **/
        Scatter() = delete;
        explicit Scatter(const std::size_t sourceID);

    public: /** Data **/
        std::vector<
            std::pair<
                std::size_t,                         // Destination computing node ID
                decltype(Messages::Scatter::m_data)  // Data to be scattered
            >
        > m_data;
    };

    class Gather : public BaseMessage {
    public: /** Construction **/
        Gather() = delete;
        explicit Gather(const std::size_t destinationID);

    public: /** Data **/
        std::vector<
            std::pair<
                std::size_t,                        // Source computing node ID
                decltype(Messages::Gather::m_data)  // Data to be gathered
            >
        > m_data;
    };

    class AllGather : public BaseMessage {
    public: /** Construction **/
        AllGather();

    public: /** Data **/
        std::vector<
            std::pair<
                std::size_t,                           // Source computing node ID
                decltype(Messages::AllGather::m_data)  // Data to be gathered
            >
        > m_data;
    };
};
