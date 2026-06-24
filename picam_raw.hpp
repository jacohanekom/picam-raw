#pragma once
// picam_raw.hpp — Pi Camera 3 raw YUV420 UDP server (Raspberry Pi 5)
//
// Sends uncompressed YUV420 frames over UDP to registered clients.
// Each frame is split into fixed-size chunks.
//
// Chunk 0 carries a 32-byte header with embedded metadata:
//
//   [frame_seq    : uint32_t BE]   bytes  0- 3
//   [chunk_seq    : uint16_t BE]   bytes  4- 5   (always 0 for chunk 0)
//   [total_chunks : uint16_t BE]   bytes  6- 7
//   [timestamp_us : int64_t  BE]   bytes  8-15   wall-clock µs (CLOCK_REALTIME)
//   [camera_index : uint8_t     ]  byte  16
//   [camera_label : char[15]    ]  bytes 17-31   null-padded ASCII
//
// Chunks 1+ carry the standard 8-byte header:
//
//   [frame_seq    : uint32_t BE]   bytes  0- 3
//   [chunk_seq    : uint16_t BE]   bytes  4- 5
//   [total_chunks : uint16_t BE]   bytes  6- 7
//   [payload ...]
//
// Clients register by sending any UDP datagram to the server port.
// They are removed after a configurable idle timeout.
//
// Two ports are served simultaneously:
//   raw_main_port  — full-res stream  (default 8560)
//   raw_lores_port — lores stream     (default 8561)
//
// Dependencies:
//   sudo apt install libcamera-dev
//
// Build:
//   cmake -B build && cmake --build build -j4

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

// ---------------------------------------------------------------------------
// Chunk header constants
// ---------------------------------------------------------------------------

static constexpr size_t kChunkHeaderSize   = 8;   // all chunks
static constexpr size_t kChunk0HeaderSize  = 32;  // chunk 0 only (includes metadata)
static constexpr size_t kLabelSize         = 15;  // camera_label field width

// Metadata extracted from chunk 0 header
struct FrameMeta {
    int64_t  timestampUs  = 0;
    uint8_t  cameraIndex  = 0;
    char     cameraLabel[kLabelSize + 1] = {};  // +1 for null terminator
};

// ---------------------------------------------------------------------------
// Per-camera configuration
// ---------------------------------------------------------------------------

struct CameraConfig {
    int         index        = 0;
    std::string label        = "cam0";
    int         mainWidth    = 2304;
    int         mainHeight   = 1296;
    int         loresWidth   = 640;
    int         loresHeight  = 360;
    int         framerate    = 30;
    bool        hflip        = false;
    bool        vflip        = false;
};

// ---------------------------------------------------------------------------
// Global configuration
// ---------------------------------------------------------------------------

struct Config {
    int rawMainPort    = 8560;
    int rawLoresPort   = 8561;
    int telemetryPort  = 8555;
    int commandPort    = 8556;
    int udpChunkSize   = 60000;   // bytes of YUV payload per UDP packet
    int clientTimeoutS = 10;      // seconds before an idle client is removed
    int cpuCore        = 0;

    std::vector<CameraConfig> cameras;
    int                       activeCamera = 0;
    int                       luxCamera    = 0;
    std::string               stateFile    = "/var/lib/picam-raw/state";
};

extern Config g_config;

bool loadConfig(int argc, char* argv[]);

// ---------------------------------------------------------------------------
// Application state
// ---------------------------------------------------------------------------

struct AppState {
    std::mutex  mu;
    float       lux          = 0.0f;
    int         activeCamera = 0;
};

extern AppState g_state;

extern std::function<void(int)> g_switchCamera;

// ---------------------------------------------------------------------------
// Raw YUV frame
// ---------------------------------------------------------------------------

struct RawFrame {
    std::vector<uint8_t> data;      // packed YUV420: Y then U then V
    int                  width       = 0;
    int                  height      = 0;
    int                  stride      = 0;
    int64_t              timestampUs = 0;
    int                  cameraIndex = 0;
    std::string          cameraLabel;
};

// ---------------------------------------------------------------------------
// FrameQueue
// ---------------------------------------------------------------------------

class FrameQueue {
public:
    explicit FrameQueue(size_t capacity = 4);

    void                    push(RawFrame frame);
    std::optional<RawFrame> pop(int timeoutMs = 500);
    void                    clear();

private:
    size_t                   capacity_;
    std::queue<RawFrame>     q_;
    std::mutex               mu_;
    std::condition_variable  cv_;
};

// ---------------------------------------------------------------------------
// UdpRawServer
//
// Binds a UDP port.  Any incoming datagram registers the sender as a
// client.  Frames are delivered by calling pushFrame() from the camera
// callback thread; the server splits them into chunks and sends to all
// registered clients.
//
// Chunk header layout (8 bytes, big-endian):
//   uint32_t  frame_seq
//   uint16_t  chunk_seq
//   uint16_t  total_chunks
// ---------------------------------------------------------------------------

class UdpRawServer {
public:
    UdpRawServer(int port, int chunkSize, int clientTimeoutS,
                 std::string name);
    ~UdpRawServer();

    void start();
    void pushFrame(RawFrame frame);

    int clientCount() const;

private:
    void listenLoop();   // registers clients from incoming datagrams
    void sendLoop();     // dequeues frames and blasts chunks

    void sendFrameToClients(const RawFrame& frame);

    const int         port_;
    const int         chunkSize_;
    const int         timeoutS_;
    const std::string name_;
    int               sock_ = -1;

    struct Client {
        sockaddr_in  addr;
        std::chrono::steady_clock::time_point lastSeen;
    };
    std::map<std::string, Client> clients_;   // key = "ip:port"
    mutable std::mutex            clientsMu_;

    FrameQueue  queue_;
    std::thread listenThread_;
    std::thread sendThread_;
    std::atomic<bool> running_{false};
    std::atomic<uint32_t> frameSeq_{0};

    // FPS tracking
    std::atomic<int>     fpsCount_{0};
    std::chrono::steady_clock::time_point fpsWindow_{std::chrono::steady_clock::now()};
};

// ---------------------------------------------------------------------------
// Telemetry / command servers
// ---------------------------------------------------------------------------

class TelemetryServer {
public:
    explicit TelemetryServer(int port);
    void start();
    void broadcast(float lux, int activeCam);
private:
    void acceptLoop();
    int              port_;
    std::thread      thread_;
    std::vector<int> clients_;
    std::mutex       clientsMu_;
};

extern TelemetryServer* g_telemetry;

class CommandServer {
public:
    explicit CommandServer(int port);
    void start();
private:
    void acceptLoop();
    void handleClient(int fd);
    int         port_;
    std::thread thread_;
};

// ---------------------------------------------------------------------------
// CameraManager
// ---------------------------------------------------------------------------

class CameraManager {
public:
    CameraManager(std::shared_ptr<UdpRawServer> mainServer,
                  std::shared_ptr<UdpRawServer> loresServer);
    ~CameraManager();

    bool start();
    void stop();
    void switchTo(int cameraIndex);

private:
    struct Impl;
    std::unique_ptr<Impl>           impl_;
    std::shared_ptr<UdpRawServer>   mainServer_;
    std::shared_ptr<UdpRawServer>   loresServer_;
};
