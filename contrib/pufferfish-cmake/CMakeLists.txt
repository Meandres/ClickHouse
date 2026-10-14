option (ENABLE_PUFFERFISH "Enable the PufferFish format-aware object storage cache" ${ENABLE_LIBRARIES})

if (NOT ENABLE_PUFFERFISH)
    message (STATUS "Not using PufferFish")
    return ()
endif ()

if (NOT PUFFERFISH_SOURCE_DIR)
    set (PUFFERFISH_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/pufferfish")
endif ()
message (STATUS "Using PufferFish from ${PUFFERFISH_SOURCE_DIR}")

# Built from source with ClickHouse's toolchain and libc++: libraries from pufferfish/build use the system C++ library.
# The S3 store (libcurl) is left out; ClickHouse provides the ObjectStore over its own S3 client.
set (PUFFERFISH_SRC "${PUFFERFISH_SOURCE_DIR}/src")
add_library (_pufferfish
    "${PUFFERFISH_SRC}/core/status.cc"
    "${PUFFERFISH_SRC}/core/buffer.cc"
    "${PUFFERFISH_SRC}/core/thread_pool.cc"
    "${PUFFERFISH_SRC}/layout/layout.cc"
    "${PUFFERFISH_SRC}/cache/planner.cc"
    "${PUFFERFISH_SRC}/cache/entry_store.cc"
    "${PUFFERFISH_SRC}/cache/lru_policy.cc"
    "${PUFFERFISH_SRC}/cache/cache.cc")
target_include_directories (_pufferfish SYSTEM PUBLIC "${PUFFERFISH_SOURCE_DIR}/include")
target_include_directories (_pufferfish PRIVATE "${PUFFERFISH_SRC}")
add_library (ch_contrib::pufferfish ALIAS _pufferfish)
