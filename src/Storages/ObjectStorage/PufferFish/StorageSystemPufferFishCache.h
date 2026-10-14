#pragma once

#include "config.h"

#if USE_PUFFERFISH

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// system.pufferfish_cache: the options and Cache::Stats() of the process-wide PufferFish cache, one row per value.
class StorageSystemPufferFishCache final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemPufferFishCache"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}

#endif
