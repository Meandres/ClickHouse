#pragma once

#include "config.h"

#if USE_PUFFERFISH

#include <memory>
#include <mutex>

#include <pufferfish/cache.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

/// The process-wide PufferFish cache, created on first use from the server config section <pufferfish>.
class PufferFishCache
{
public:
    static PufferFishCache & instance();

    std::shared_ptr<pufferfish::Cache> getOrInit(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<pufferfish::Cache> tryGet() const;

private:
    mutable std::mutex mutex;
    std::shared_ptr<pufferfish::Cache> cache;
};

}

#endif
