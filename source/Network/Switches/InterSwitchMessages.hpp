#pragma once

#include "Network/Message.hpp"
#include <string>

namespace Network::Messages::InterSwitch {
    class Gather : public BaseMessage {
    public: /** Construction **/
        Gather() = delete;
        explicit Gather(const std::size_t destinationID);

    public: /** Addressing **/
        const std::size_t m_destinationID;

    public: /** Data **/
        std::vector<
            std::pair<
                std::size_t,                        // Computing node ID
                decltype(Messages::Gather::m_data)  // Data to be gathered
            >
        > m_data;
    };
};
