#include <Storages/ObjectStorage/PufferFish/PufferFishReadBuffer.h>

#if USE_PUFFERFISH

#include <Storages/ObjectStorage/PufferFish/PufferFishCache.h>
#include <Common/Exception.h>
#include <Common/FullyQualifiedObjectPath.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/ProfileEvents.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Interpreters/Context.h>

#if USE_AWS_S3
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/S3Settings.h>
#include <aws/core/http/HttpResponse.h>
#endif

#include <algorithm>

namespace ProfileEvents
{
    extern const Event PufferFishReadMicroseconds;
    extern const Event PufferFishReadBytes;
    extern const Event PufferFishGetRequests;
    extern const Event PufferFishGetBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int S3_ERROR;
    extern const int S3_OBJECT_CHANGED_DURING_READ;
}

PufferFishReadBuffer::PufferFishReadBuffer(std::shared_ptr<pufferfish::File> file_, String file_name_, size_t buf_size)
    : ReadBufferFromFileBase(std::max<size_t>(1, std::min<size_t>(buf_size, file_->size())), nullptr, 0, file_->size())
    , file(std::move(file_))
    , file_name(std::move(file_name_))
{
}

void PufferFishReadBuffer::readAt(char * to, size_t n, size_t range_begin) const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PufferFishReadMicroseconds);
    pufferfish::Status status;
    {
        /// Entries outlive the query that fetched them.
        MemoryTrackerSwitcher switcher{&total_memory_tracker};
        status = file->ReadAt(range_begin, {reinterpret_cast<std::byte *>(to), n});
    }
    if (!status.ok())
        throw Exception(
            status.code() == pufferfish::StatusCode::kPreconditionFailed ? ErrorCodes::S3_OBJECT_CHANGED_DURING_READ : ErrorCodes::S3_ERROR,
            "PufferFish read of {} [{}, {}) failed: {}", file_name, range_begin, range_begin + n, status.ToString());
    ProfileEvents::increment(ProfileEvents::PufferFishReadBytes, n);
}

size_t PufferFishReadBuffer::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const
{
    const size_t size = file->size();
    if (range_begin >= size)
        return 0;
    n = std::min(n, size - range_begin);
    readAt(to, n, range_begin);
    if (progress_callback)
        progress_callback(n);
    return n;
}

bool PufferFishReadBuffer::nextImpl()
{
    const size_t size = file->size();
    const size_t end = read_until_position ? std::min(read_until_position, size) : size;
    if (offset >= end)
        return false;
    const size_t n = std::min(internal_buffer.size(), end - offset);
    readAt(internal_buffer.begin(), n, offset);
    working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
    offset += n;
    return true;
}

off_t PufferFishReadBuffer::getPosition()
{
    return offset - available();
}

off_t PufferFishReadBuffer::seek(off_t off, int whence)
{
    off_t new_pos;
    if (whence == SEEK_SET)
        new_pos = off;
    else if (whence == SEEK_CUR)
        new_pos = getPosition() + off;
    else
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET and SEEK_CUR are supported");
    if (new_pos < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds: {}", new_pos);

    const off_t buffer_end = offset;
    const off_t buffer_begin = buffer_end - working_buffer.size();
    if (!working_buffer.empty() && new_pos >= buffer_begin && new_pos <= buffer_end)
    {
        pos = working_buffer.begin() + (new_pos - buffer_begin);
        return new_pos;
    }
    resetWorkingBuffer();
    offset = new_pos;
    return new_pos;
}

void PufferFishReadBuffer::setReadUntilPosition(size_t position)
{
    if (position == read_until_position)
        return;
    offset = getPosition();
    resetWorkingBuffer();
    read_until_position = position;
}

void PufferFishReadBuffer::setReadUntilEnd()
{
    setReadUntilPosition(0);
}

void PufferFishReadBuffer::addFormatUnits(std::string_view format, Units units)
{
    auto current = file->layout();
    std::vector<pufferfish::ByteRange> merged;
    if (current && current->format() == format)
        merged.assign(current->units().begin(), current->units().end());
    const auto known_end = merged.size();
    const auto by_offset = [](const pufferfish::ByteRange & a, const pufferfish::ByteRange & b) { return a.offset < b.offset; };

    bool changed = false;
    for (const auto & [unit_offset, unit_length] : units)
    {
        const pufferfish::ByteRange unit{unit_offset, unit_length};
        auto it = std::lower_bound(merged.begin(), merged.begin() + known_end, unit, by_offset);
        if (it != merged.begin() + known_end && *it == unit)
            continue;
        merged.push_back(unit);
        changed = true;
    }
    if (changed)
        file->SetLayout({String(format), std::move(merged)});
}

#if USE_AWS_S3

namespace S3RequestSetting
{
    extern const S3RequestSettingsUInt64 max_single_read_retries;
}

namespace
{

/// PufferFish's ObjectStore over ClickHouse's S3 client, so endpoint, credentials and retries are those of the table.
/// Keys are "bucket/key". Head of the opened object is answered from the metadata ClickHouse already has.
class ClickHouseS3Store final : public pufferfish::ObjectStore
{
public:
    ClickHouseS3Store(std::shared_ptr<const S3::Client> client_, String id_, size_t max_attempts_, String known_key_, pufferfish::ObjectInfo known_info_)
        : client(std::move(client_))
        , store_id(std::move(id_))
        , max_attempts(std::max<size_t>(1, max_attempts_))
        , known_key(std::move(known_key_))
        , known_info(std::move(known_info_))
    {
    }

    const std::string & id() const override { return store_id; }

    pufferfish::Result<pufferfish::ObjectInfo> Head(const std::string & key) override
    {
        if (key == known_key)
            return known_info;
        try
        {
            const auto [bucket, object_key] = split(key);
            auto info = S3::getObjectInfo(*client, bucket, object_key);
            return pufferfish::ObjectInfo{info.size, info.etag};
        }
        catch (...)
        {
            return pufferfish::Status::IOError(getCurrentExceptionMessage(false));
        }
    }

    pufferfish::Status Get(
        const std::string & key, const pufferfish::ObjectInfo & info, pufferfish::ByteRange range,
        std::span<const pufferfish::MutableBytes> parts) override
    {
        const auto [bucket, object_key] = split(key);
        uint64_t done = 0;
        size_t part = 0;
        size_t part_offset = 0;
        for (size_t attempt = 1;; ++attempt)
        {
            try
            {
                S3::GetObjectRequest request;
                request.SetBucket(bucket);
                request.SetKey(object_key);
                if (!info.version.empty())
                    request.SetIfMatch(info.version);
                request.SetRange(fmt::format("bytes={}-{}", range.offset + done, range.end() - 1));
                ProfileEvents::increment(ProfileEvents::PufferFishGetRequests);

                auto outcome = client->GetObject(request);
                if (!outcome.IsSuccess())
                {
                    const auto & error = outcome.GetError();
                    if (error.GetResponseCode() == Aws::Http::HttpResponseCode::PRECONDITION_FAILED)
                        return pufferfish::Status::PreconditionFailed(key + " changed: " + error.GetMessage());
                    if (!error.ShouldRetry() || attempt >= max_attempts)
                        return pufferfish::Status::IOError(key + ": " + error.GetMessage());
                    continue;
                }

                auto & body = outcome.GetResult().GetBody();
                while (done < range.length && part < parts.size())
                {
                    auto dst = parts[part].subspan(part_offset);
                    body.read(reinterpret_cast<char *>(dst.data()), dst.size());
                    const auto got = static_cast<size_t>(body.gcount());
                    if (got == 0)
                        break;
                    done += got;
                    part_offset += got;
                    if (part_offset == parts[part].size())
                    {
                        ++part;
                        part_offset = 0;
                    }
                }
                ProfileEvents::increment(ProfileEvents::PufferFishGetBytes, done);
                if (done == range.length)
                    return pufferfish::Status::OK();
                if (attempt >= max_attempts)
                    return pufferfish::Status::IOError(fmt::format("{}: short read, {} of {} bytes", key, done, range.length));
            }
            catch (...)
            {
                if (attempt >= max_attempts)
                    return pufferfish::Status::IOError(key + ": " + getCurrentExceptionMessage(false));
            }
        }
    }

private:
    static std::pair<String, String> split(const std::string & key)
    {
        const auto slash = key.find('/');
        return {key.substr(0, slash), slash == std::string::npos ? String() : key.substr(slash + 1)};
    }

    std::shared_ptr<const S3::Client> client;
    String store_id;
    size_t max_attempts;
    String known_key;
    pufferfish::ObjectInfo known_info;
};

}

std::unique_ptr<ReadBufferFromFileBase> tryCreatePufferFishReadBuffer(
    const RelativePathWithMetadata & object,
    const ObjectStoragePtr & object_storage,
    const ReadSettings & read_settings,
    const ContextPtr & context)
{
    auto * s3_storage = dynamic_cast<S3ObjectStorage *>(object_storage.get());
    if (!s3_storage || !object.metadata)
        return nullptr;

    auto cache = PufferFishCache::instance().getOrInit(context->getConfigRef());

    String bucket = s3_storage->getObjectsNamespace();
    String key = object.getPath();
    if (auto qualified = trySplitFullyQualifiedObjectPath(key))
    {
        bucket = String(qualified->object_namespace);
        key = String(qualified->key);
    }
    const String full_key = bucket + "/" + key;

    auto store = std::make_shared<ClickHouseS3Store>(
        s3_storage->getS3StorageClient(),
        "s3:" + s3_storage->getURI().endpoint,
        s3_storage->getS3Settings().request_settings[S3RequestSetting::max_single_read_retries],
        full_key,
        pufferfish::ObjectInfo{object.metadata->size_bytes, object.metadata->etag});

    auto file = cache->Open(std::move(store), full_key);
    if (!file.ok())
        throw Exception(ErrorCodes::S3_ERROR, "Cannot open {} through PufferFish: {}", full_key, file.status().ToString());

    return std::make_unique<PufferFishReadBuffer>(
        std::shared_ptr<pufferfish::File>(std::move(file).value()), full_key, read_settings.remote_fs_settings.buffer_size);
}

#else

std::unique_ptr<ReadBufferFromFileBase> tryCreatePufferFishReadBuffer(
    const RelativePathWithMetadata &, const ObjectStoragePtr &, const ReadSettings &, const ContextPtr &)
{
    return nullptr;
}

#endif

}

#endif
