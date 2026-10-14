#pragma once

#include <IO/ReadBuffer.h>
#include <base/types.h>

#include <string_view>
#include <utility>
#include <vector>

namespace DB
{

/// A read buffer whose cache wants the byte ranges of the file format's units (Parquet column chunks, ORC streams, ...).
/// Input formats push them after parsing the file metadata.
class WithFormatLayout
{
public:
    /// (offset, length) pairs, in any order.
    using Units = std::vector<std::pair<UInt64, UInt64>>;

    virtual ~WithFormatLayout() = default;

    /// Adds to the units already known for this object version; may be called several times.
    virtual void addFormatUnits(std::string_view format, Units units) = 0;
};

inline WithFormatLayout * tryGetFormatLayoutSink(ReadBuffer & in)
{
    return dynamic_cast<WithFormatLayout *>(&in);
}

}
