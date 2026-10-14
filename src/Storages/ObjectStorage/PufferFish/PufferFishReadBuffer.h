#pragma once

#include "config.h"

#if USE_PUFFERFISH

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WithFormatLayout.h>
#include <Interpreters/Context_fwd.h>

#include <pufferfish/cache.h>

namespace DB
{

/// Serves an object from a pufferfish::File: readBigAt for the Parquet and ORC readers, nextImpl/seek for sequential readers.
class PufferFishReadBuffer final : public ReadBufferFromFileBase, public WithFormatLayout
{
public:
    PufferFishReadBuffer(std::shared_ptr<pufferfish::File> file_, String file_name_, size_t buf_size);

    bool nextImpl() override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    std::optional<size_t> tryGetFileSize() override { return file->size(); }
    String getFileName() const override { return file_name; }

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;
    size_t getFileOffsetOfBufferEnd() const override { return offset; }
    bool supportsRightBoundedReads() const override { return true; }
    bool isSeekCheap() override { return true; }

    bool supportsReadAt() override { return true; }
    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const override;

    void addFormatUnits(std::string_view format, Units units) override;

private:
    void readAt(char * to, size_t n, size_t range_begin) const;

    std::shared_ptr<pufferfish::File> file;
    String file_name;
    size_t offset = 0;
    size_t read_until_position = 0;
};

/// nullptr if the object is not on S3; the caller then builds its usual read pipeline.
std::unique_ptr<ReadBufferFromFileBase> tryCreatePufferFishReadBuffer(
    const RelativePathWithMetadata & object,
    const ObjectStoragePtr & object_storage,
    const ReadSettings & read_settings,
    const ContextPtr & context);

}

#endif
