// Libraries
#include "spdlog/spdlog.h"
#include "cxxopts.hpp"
#include <vector>

// User-defined
#include "Network/Switches/Aggregate.hpp"
#include "Network/Switches/Core.hpp"
#include "Network/Switches/Edge.hpp"
#include "Network/Derivations.hpp"
#include "Computer.hpp"

int main(const int argc, const char *const argv[])
{
    cxxopts::Options options(argv[0], "In-network computing simulation based on fat-tree topology");

    options.add_options()
            ("ports",
             "Ports per switch(4, 6, ..., 2n)",
             cxxopts::value<std::size_t>()->default_value(std::to_string(4)))

            ("log-filter",
             "Level of log filter"
             "\n0: Trace"
             "\n1: Debug"
             "\n2: Info"
             "\n3: Warning"
             "\n4: Error"
             "\n5: Critical"
             "\n6: No log",
             cxxopts::value<int>()->default_value(std::to_string(spdlog::level::info)))

            ("network-computing",
             "Enable in-network computing",
             cxxopts::value<bool>()->default_value("true"))

            ("help", "Print help");

    cxxopts::ParseResult arguments;

    try {
        arguments = options.parse(argc, argv);

        if(arguments.count("help")) {
            if(arguments.arguments().size() > 1) {
                spdlog::warn("Help requested, ignoring {} other arguments!", arguments.arguments().size() - 1);
            }

            spdlog::info(options.help());

            return 0;
        }
    }
    catch(const cxxopts::exceptions::exception& e) {
        spdlog::critical("Couldn't parse arguments! {}", e.what());

        return -1;
    }

    const bool bInNetworkComputing = arguments["network-computing"].as<bool>();
    const std::size_t portPerSwitch = arguments["ports"].as<std::size_t>();
    spdlog::set_level(spdlog::level::level_enum(arguments["log-filter"].as<int>()));
    spdlog::info("Starting program..");

    if(4 > portPerSwitch) {
        spdlog::critical("Port per switch cannot be less than 4!");

        return  -1;
    }
    else if(0 != (portPerSwitch % 2)) {
        spdlog::critical("Port per switch({}) must be an exact multiple of 2!", portPerSwitch);

        return  -1;
    }
    else {
        spdlog::debug("Port per switch determined as {}", portPerSwitch);
    }

    Network::Switches::setNetworkComputing(bInNetworkComputing);
    if(Network::Switches::isNetworkComputingEnabled()) {
        spdlog::info("In-network computing is enabled!");
    }
    else {
        spdlog::warn("In-network computing is disabled!");
    }

    // Derived constants
    const std::size_t coreSwitchAmount      = Network::Utilities::deriveCoreSwitchAmount(portPerSwitch);
    const std::size_t aggregateSwitchAmount = Network::Utilities::deriveAggregateSwitchAmount(portPerSwitch);
    const std::size_t edgeSwitchAmount      = Network::Utilities::deriveEdgeSwitchAmount(portPerSwitch);
    const std::size_t compNodeAmount        = Network::Utilities::deriveComputingNodeAmount(portPerSwitch);

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

        if(std::all_of(computeNodes.cbegin(), computeNodes.cend(), [](const auto &compNode) { return compNode.isDone(); })) {
            spdlog::info("All computing nodes have finished their tasks!");

            break;
        }
    }

    spdlog::warn("Program finished after {} ticks!", tick);

    return 0;
}
