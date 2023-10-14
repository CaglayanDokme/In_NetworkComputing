#include <vector>
#include "Computer.hpp"
#include "Network/Switches/Core.hpp"
#include "Network/Switches/Aggregate.hpp"
#include "Network/Switches/Edge.hpp"
#include "spdlog/spdlog.h"

int main()
{
    spdlog::set_level(spdlog::level::trace);
    spdlog::info("Starting program..");

    const std::size_t portPerSwitch = 4; // TODO Parse from command line

    if(0 == portPerSwitch) {
        spdlog::critical("Port per switch cannot be zero!");

        return  -1;
    }

    if(0 != (portPerSwitch % 2)) {
        spdlog::critical("Port per switch({}) must be an exact multiple of 2!");

        return  -1;
    }

    // Derived constants
    const std::size_t coreSwitchAmount      = std::pow(portPerSwitch, 2) / 4;
    const std::size_t aggregateSwitchAmount = coreSwitchAmount * 2;
    const std::size_t edgeSwitchAmount      = aggregateSwitchAmount;
    const std::size_t compNodeAmount        = edgeSwitchAmount * (portPerSwitch / 2);
    const std::size_t portAmount            = compNodeAmount + (portPerSwitch * (coreSwitchAmount + aggregateSwitchAmount + edgeSwitchAmount));

    // Nodes (Switch & Compute)
    std::vector<Network::Switches::Core> coreSwitches;
    std::vector<Network::Switches::Aggregate> aggSwitches;
    std::vector<Network::Switches::Edge> edgeSwitches;

    Computer::setTotalAmount(compNodeAmount);
    std::vector<Computer> computeNodes(compNodeAmount);
    spdlog::debug("Generated {} computing nodes in total.", computeNodes.size());

    // Generate switches
    {
        // Generate core switches
        coreSwitches.reserve(coreSwitchAmount);
        for(size_t coreSwIdx = 0; coreSwIdx  < coreSwitchAmount; ++coreSwIdx) {
            coreSwitches.emplace_back(portPerSwitch);
        }
        spdlog::debug("Generated {} core switches in total.", coreSwitches.size());

        // Generate aggregate switches
        aggSwitches.reserve(aggregateSwitchAmount);
        for(size_t aggSwIdx = 0; aggSwIdx < aggregateSwitchAmount; ++aggSwIdx) {
            aggSwitches.emplace_back(portPerSwitch);
        }
        spdlog::debug("Generated {} aggregate switches in total.", aggSwitches.size());

        // Generate edge switches
        edgeSwitches.reserve(edgeSwitchAmount);
        for(size_t edgeSwIdx = 0; edgeSwIdx < edgeSwitchAmount; ++edgeSwIdx) {
            edgeSwitches.emplace_back(portPerSwitch);
        }
        spdlog::debug("Generated {} edge switches in total.", edgeSwitches.size());
    }

    // Link core and aggregate switches
    {
        if(aggregateSwitchAmount != (coreSwitchAmount * 2)) {
            spdlog::error("Aggregate({}) and core({}) switch amounts must have a ratio of 2!", aggregateSwitchAmount, edgeSwitchAmount);

            throw std::logic_error("Switch amounts doesn't match!");
        }

        const size_t upPortPerSwitch = portPerSwitch / 2;
        const size_t aggGroupAmount  = portPerSwitch;
        const size_t aggGroupSize    = aggregateSwitchAmount / aggGroupAmount;
        for(size_t aggSwIdx = 0; aggSwIdx < aggregateSwitchAmount; ++aggSwIdx) {
            const size_t firstCoreSwIdx = (aggSwIdx % aggGroupSize) * upPortPerSwitch;
            const size_t corePortIdx = aggSwIdx / aggGroupSize;
            auto &aggSw = aggSwitches.at(aggSwIdx);

            for(size_t aggUpPortIdx = 0; aggUpPortIdx < upPortPerSwitch; ++aggUpPortIdx) {
                const auto coreSwIdx = firstCoreSwIdx + aggUpPortIdx;
                auto &coreSw = coreSwitches.at(coreSwIdx);

                if(!aggSw.getUpPort(aggUpPortIdx).connect(coreSw.getPort(corePortIdx))) {
                    spdlog::error("Couldn't connect core switch #{} with aggregate switch #{}!", coreSwIdx, aggSwIdx);
                }
                else {
                    spdlog::trace("Connected core switch #{} with aggregate switch #{}.", coreSwIdx, aggSwIdx);
                }
            }
        }
    }

    // Link aggregate and edge switches
    {
        if(aggregateSwitchAmount != edgeSwitchAmount) {
            spdlog::error("Aggregate({}) and edge({}) switch amounts must have been equal!", aggregateSwitchAmount, edgeSwitchAmount);

            throw std::logic_error("Switch amounts doesn't match!");
        }

        const size_t upPortPerSwitch = portPerSwitch / 2;
        const size_t downPortPerSwitch = upPortPerSwitch;
        const size_t groupAmount  = portPerSwitch;
        const size_t groupSize    = aggregateSwitchAmount / groupAmount;

        for(size_t aggSwIdx = 0; aggSwIdx < aggregateSwitchAmount; ++aggSwIdx) {
            const size_t edgeUpPortIdx = (aggSwIdx % groupSize);
            const size_t firstEdgeSwIdx = aggSwIdx - (aggSwIdx % groupSize);

            auto &aggSw = aggSwitches.at(aggSwIdx);
            for(size_t edgeSwIdx = firstEdgeSwIdx; edgeSwIdx < (firstEdgeSwIdx + groupSize); ++edgeSwIdx) {
                if(!aggSw.getDownPort(edgeSwIdx - firstEdgeSwIdx).connect(edgeSwitches.at(edgeSwIdx).getUpPort(edgeUpPortIdx))) {
                    spdlog::error("Couldn't connect edge switch #{} with aggregate switch #{}!", edgeSwIdx, aggSwIdx);
                }
                else {
                    spdlog::trace("Connected edge switch #{} with aggregate switch #{}.", edgeSwIdx, aggSwIdx);
                }
            }
        }
    }

    // Link edge switches and computing nodes
    {
        const size_t downPortPerSwitch = portPerSwitch / 2;

        for(size_t edgeSwIdx = 0; edgeSwIdx < edgeSwitchAmount; ++edgeSwIdx) {
            auto &edgeSw = edgeSwitches.at(edgeSwIdx);

            for(size_t downPortIdx = 0; downPortIdx < downPortPerSwitch; ++downPortIdx) {
                const auto compNodeIdx = (edgeSwIdx * downPortPerSwitch) + downPortIdx;

                if(!edgeSw.getDownPort(downPortIdx).connect(computeNodes.at(compNodeIdx).getPort())) {
                    spdlog::error("Couldn't connect edge switch #{} with computing node #{}!", edgeSwIdx, compNodeIdx);
                }
                else {
                    spdlog::trace("Connected edge switch #{} with computing node #{}.", edgeSwIdx, compNodeIdx);
                }
            }
        }
    }

    // Check network's establishment status
    {
        for(const auto &coreSw : coreSwitches) {
            if(!coreSw.isReady()) {
                spdlog::error("Couldn't establish network as core switch #{} isn't ready!", coreSw.getID());

                throw std::logic_error("Couldn't establish network!");
            }
        }

        for(const auto &aggSw : aggSwitches) {
            if(!aggSw.isReady()) {
                spdlog::error("Couldn't establish network as aggregate switch #{} isn't ready!", aggSw.getID());

                throw std::logic_error("Couldn't establish network!");
            }
        }

        for(const auto &edgeSw : edgeSwitches) {
            if(!edgeSw.isReady()) {
                spdlog::error("Couldn't establish network as edge switch isn't ready!", edgeSw.getID());

                throw std::logic_error("Couldn't establish network!");
            }
        }

        for(const auto &compNode : computeNodes) {
            if(!compNode.isReady()) {
                spdlog::error("Couldn't establish network as computing node #{} isn't ready!", compNode.getID());

                throw std::logic_error("Couldn't establish network!");
            }
        }
    }
    spdlog::info("Network established successfully!");

    std::size_t tick = 0;
    while(++tick) {
        spdlog::trace("Tick #{}", tick);

        for(auto &coreSw : coreSwitches) {
            if(!coreSw.tick()) {
                spdlog::error("Tick #{} failed for core switch #{}!", tick, coreSw.getID());
            }
        }

        for(auto &aggSw : aggSwitches) {
            if(!aggSw.tick()) {
                spdlog::error("Tick #{} failed for aggregate switch #{}!", tick, aggSw.getID());
            }
        }

        for(auto &edgeSw : edgeSwitches) {
            if(!edgeSw.tick()) {
                spdlog::error("Tick #{} failed for edge switch #{}!", tick, edgeSw.getID());
            }
        }

        for(auto &compNode : computeNodes) {
            if(!compNode.tick()) {
                spdlog::error("Tick #{} failed for computing node #{}!", tick, compNode.getID());
            }
        }
    }

    spdlog::warn("Program finished!");

    return 0;
}
