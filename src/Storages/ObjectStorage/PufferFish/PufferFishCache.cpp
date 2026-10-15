#include <Storages/ObjectStorage/PufferFish/PufferFishCache.h>

#if USE_PUFFERFISH

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

PufferFishCache & PufferFishCache::instance()
{
    static PufferFishCache holder;
    return holder;
}

std::shared_ptr<pufferfish::Cache> PufferFishCache::getOrInit(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(mutex);
    if (cache)
        return cache;

    pufferfish::CacheOptions options;
    const String prefix = "pufferfish.";
    options.capacity_bytes = config.getUInt64(prefix + "capacity_bytes", options.capacity_bytes);
    options.format_aware = config.getBool(prefix + "format_aware", options.format_aware);
    options.block_size = config.getUInt64(prefix + "block_size", options.block_size);
    options.coalesce = config.getBool(prefix + "coalesce", options.coalesce);
    options.max_get_bytes = config.getUInt64(prefix + "max_get_bytes", options.max_get_bytes);
    options.trace_path = config.getString(prefix + "trace_path", "");

    auto made = pufferfish::Cache::Make(options);
    if (!made.ok())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create the PufferFish cache: {}", made.status().ToString());
    cache = std::move(made).value();

    LOG_INFO(getLogger("PufferFishCache"), "PufferFish cache: {} bytes, format_aware {}, block_size {}, coalesce {}",
        options.capacity_bytes, options.format_aware, options.block_size, options.coalesce);
    return cache;
}

std::shared_ptr<pufferfish::Cache> PufferFishCache::tryGet() const
{
    std::lock_guard lock(mutex);
    return cache;
}

}

#endif
