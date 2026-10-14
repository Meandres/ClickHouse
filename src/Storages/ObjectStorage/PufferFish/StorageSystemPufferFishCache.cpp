#include <Storages/ObjectStorage/PufferFish/StorageSystemPufferFishCache.h>

#if USE_PUFFERFISH

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/PufferFish/PufferFishCache.h>

namespace DB
{

ColumnsDescription StorageSystemPufferFishCache::getColumnsDescription()
{
    return ColumnsDescription{
        {"name", std::make_shared<DataTypeString>(), "Option or statistic of the PufferFish cache."},
        {"value", std::make_shared<DataTypeUInt64>(), "Its value. Statistics count since the cache was created or last cleared."},
    };
}

void StorageSystemPufferFishCache::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto cache = PufferFishCache::instance().tryGet();
    if (!cache)
        return;
    auto add = [&](const char * name, UInt64 value)
    {
        res_columns[0]->insert(name);
        res_columns[1]->insert(value);
    };
    const auto & options = cache->options();
    add("capacity_bytes", options.capacity_bytes);
    add("format_aware", options.format_aware);
    add("block_size", options.block_size);
    add("coalesce", options.coalesce);
    add("max_get_bytes", options.max_get_bytes);
    add("io_threads", options.io_threads);
    const auto stats = cache->Stats();
    add("reads", stats.reads);
    add("bytes_read", stats.bytes_read);
    add("entry_hits", stats.entry_hits);
    add("entry_misses", stats.entry_misses);
    add("inflight_waits", stats.inflight_waits);
    add("bytes_from_cache", stats.bytes_from_cache);
    add("bytes_fetched", stats.bytes_fetched);
    add("get_requests", stats.get_requests);
    add("head_requests", stats.head_requests);
    add("evictions", stats.evictions);
    add("resident_bytes", stats.resident_bytes);
    add("resident_entries", stats.resident_entries);
    add("objects_opened", stats.objects_opened);
    add("layouts_set", stats.layouts_set);
}

}

#endif
