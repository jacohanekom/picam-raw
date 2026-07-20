// picam_raw.cpp — Pi Camera 3 raw YUV420 UDP server (Raspberry Pi 5)
//
// Build:
//   cmake -B build && cmake --build build -j4
//   ./build/picam_raw

#include "picam_raw.hpp"

#include <algorithm>
#include <cmath>
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sched.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

// libcamera
#include <libcamera/camera.h>
#include <libcamera/camera_manager.h>
#include <libcamera/control_ids.h>
#include <libcamera/controls.h>
#include <libcamera/formats.h>
#include <libcamera/framebuffer_allocator.h>
#include <libcamera/orientation.h>
#include <libcamera/request.h>
#include <libcamera/stream.h>

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------

Config           g_config;
AppState         g_state;
TelemetryServer* g_telemetry  = nullptr;
std::function<void(int)> g_switchCamera;

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

namespace {

std::mutex g_logMu;

void logMsg(const char* level, const std::string& msg) {
    auto now = std::chrono::system_clock::now();
    auto t   = std::chrono::system_clock::to_time_t(now);
    std::lock_guard lock(g_logMu);
    std::cout << std::put_time(std::localtime(&t), "%F %T")
              << " [" << level << "] " << msg << "\n" << std::flush;
}

void logInfo (const std::string& m) { logMsg("INFO ", m); }
void logWarn (const std::string& m) { logMsg("WARN ", m); }
void logError(const std::string& m) { logMsg("ERROR", m); }

std::string localIp() {
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return "<pi-ip>";
    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port   = htons(53);
    ::inet_pton(AF_INET, "8.8.8.8", &dst.sin_addr);
    ::connect(fd, reinterpret_cast<sockaddr*>(&dst), sizeof(dst));
    sockaddr_in src{};
    socklen_t   len = sizeof(src);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&src), &len);
    ::close(fd);
    char buf[INET_ADDRSTRLEN];
    ::inet_ntop(AF_INET, &src.sin_addr, buf, sizeof(buf));
    return buf;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// FrameQueue
// ---------------------------------------------------------------------------

FrameQueue::FrameQueue(size_t capacity) : capacity_(capacity) {}

void FrameQueue::push(RawFrame frame) {
    std::lock_guard lock(mu_);
    if (q_.size() >= capacity_) q_.pop();  // drop oldest if full
    q_.push(std::move(frame));
    cv_.notify_one();
}

std::optional<RawFrame> FrameQueue::pop(int timeoutMs) {
    std::unique_lock lock(mu_);
    if (cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                     [this]{ return !q_.empty(); })) {
        RawFrame f = std::move(q_.front());
        q_.pop();
        return f;
    }
    return std::nullopt;
}

void FrameQueue::clear() {
    std::lock_guard lock(mu_);
    while (!q_.empty()) q_.pop();
}

// ---------------------------------------------------------------------------
// UdpRawServer
// ---------------------------------------------------------------------------

UdpRawServer::UdpRawServer(int port, int chunkSize, int clientTimeoutS,
                             std::string name)
    : port_(port), chunkSize_(chunkSize), timeoutS_(clientTimeoutS),
      name_(std::move(name)), queue_(4)
{}

UdpRawServer::~UdpRawServer() {
    running_ = false;
    queue_.clear();
    if (sock_ >= 0) { ::close(sock_); sock_ = -1; }
    if (listenThread_.joinable()) listenThread_.join();
    if (sendThread_.joinable())   sendThread_.join();
}

void UdpRawServer::start() {
    sock_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_ < 0)
        throw std::runtime_error(name_ + ": socket() failed: " +
                                 strerror(errno));

    // Allow socket reuse
    int opt = 1;
    ::setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Increase send buffer to 8 MB to avoid drops on large frames
    int sndbuf = 8 * 1024 * 1024;
    ::setsockopt(sock_, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(port_));
    if (::bind(sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throw std::runtime_error(name_ + ": bind() failed: " +
                                 strerror(errno));

    running_ = true;
    listenThread_ = std::thread(&UdpRawServer::listenLoop, this);
    sendThread_   = std::thread(&UdpRawServer::sendLoop,   this);
    logInfo(name_ + " UDP raw server listening on :" + std::to_string(port_));
}

// Register / refresh clients from any incoming datagram.
void UdpRawServer::listenLoop() {
    char buf[64];
    while (running_) {
        sockaddr_in from{};
        socklen_t   fromLen = sizeof(from);
        ssize_t n = ::recvfrom(sock_, buf, sizeof(buf), 0,
                               reinterpret_cast<sockaddr*>(&from), &fromLen);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            if (!running_) break;
            logWarn(name_ + " recvfrom error: " + strerror(errno));
            continue;
        }

        char ipBuf[INET_ADDRSTRLEN];
        ::inet_ntop(AF_INET, &from.sin_addr, ipBuf, sizeof(ipBuf));
        std::string key = std::string(ipBuf) + ":" +
                          std::to_string(ntohs(from.sin_port));

        std::lock_guard lock(clientsMu_);
        auto now = std::chrono::steady_clock::now();
        if (clients_.find(key) == clients_.end())
            logInfo(name_ + " client registered: " + key);
        clients_[key] = Client{from, now};
    }
}

void UdpRawServer::sendLoop() {
    while (running_) {
        auto maybeFrame = queue_.pop(500);
        if (!maybeFrame) continue;
        sendFrameToClients(*maybeFrame);
    }
}

// Split a YUV420 frame into chunkSize_ chunks and send to every
// registered client.
//
// Chunk 0: 32-byte header  [frame_seq:4][chunk_seq:2][total_chunks:2]
//                          [timestamp_us:8][camera_index:1][camera_label:15]
// Chunk N: 8-byte header   [frame_seq:4][chunk_seq:2][total_chunks:2]
void UdpRawServer::sendFrameToClients(const RawFrame& frame) {
    const uint8_t* src    = frame.data.data();
    int            total  = static_cast<int>(frame.data.size());
    int            nChunk = (total + chunkSize_ - 1) / chunkSize_;
    if (nChunk > 65535) {
        logWarn(name_ + " frame too large (" + std::to_string(total) +
                " bytes) — skipping");
        return;
    }

    // FPS counter — print once per 30 seconds
    {
        int count = fpsCount_.fetch_add(1) + 1;
        auto now  = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - fpsWindow_).count();
        if (elapsed >= 30000) {
            float fps = count * 1000.0f / static_cast<float>(elapsed);
            logInfo(name_ + " " + std::to_string(static_cast<int>(std::round(fps))) + " fps");
            fpsCount_  = 0;
            fpsWindow_ = now;
        }
    }

    uint32_t seq = frameSeq_.fetch_add(1);

    // Build metadata for chunk 0 extended header
    int64_t tsUs = frame.timestampUs;
    uint8_t camIdx = static_cast<uint8_t>(frame.cameraIndex);
    char    label[kLabelSize] = {};
    std::strncpy(label, frame.cameraLabel.c_str(), kLabelSize);

    struct Chunk {
        uint8_t        header[kChunk0HeaderSize];  // max possible header size
        size_t         headerLen;
        const uint8_t* payload;
        int            payloadLen;
    };
    std::vector<Chunk> chunks;
    chunks.reserve(static_cast<size_t>(nChunk));

    for (int i = 0; i < nChunk; ++i) {
        int offset = i * chunkSize_;
        int plen   = std::min(chunkSize_, total - offset);
        Chunk c{};

        // Common 8-byte base header
        c.header[0] = (seq >> 24) & 0xFF;
        c.header[1] = (seq >> 16) & 0xFF;
        c.header[2] = (seq >>  8) & 0xFF;
        c.header[3] =  seq        & 0xFF;
        c.header[4] = (i    >>  8) & 0xFF;
        c.header[5] =  i           & 0xFF;
        c.header[6] = (nChunk >> 8) & 0xFF;
        c.header[7] =  nChunk       & 0xFF;

        if (i == 0) {
            // Extended metadata block in chunk 0 only (bytes 8-31)
            c.header[ 8] = (tsUs >> 56) & 0xFF;
            c.header[ 9] = (tsUs >> 48) & 0xFF;
            c.header[10] = (tsUs >> 40) & 0xFF;
            c.header[11] = (tsUs >> 32) & 0xFF;
            c.header[12] = (tsUs >> 24) & 0xFF;
            c.header[13] = (tsUs >> 16) & 0xFF;
            c.header[14] = (tsUs >>  8) & 0xFF;
            c.header[15] =  tsUs        & 0xFF;
            c.header[16] = camIdx;
            std::memcpy(&c.header[17], label, kLabelSize);
            c.headerLen = kChunk0HeaderSize;
        } else {
            c.headerLen = kChunkHeaderSize;
        }

        c.payload    = src + offset;
        c.payloadLen = plen;
        chunks.push_back(c);
    }

    // Prune timed-out clients while we hold the lock, then blast
    auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(clientsMu_);

    for (auto it = clients_.begin(); it != clients_.end(); ) {
        auto age = std::chrono::duration_cast<std::chrono::seconds>(
            now - it->second.lastSeen).count();
        if (age > timeoutS_) {
            logInfo(name_ + " client timed out: " + it->first);
            it = clients_.erase(it);
        } else {
            ++it;
        }
    }

    if (clients_.empty()) return;

    // Send all chunks to all clients using sendmsg for zero-copy
    for (const auto& chunk : chunks) {
        struct iovec iov[2];
        iov[0].iov_base = const_cast<uint8_t*>(chunk.header);
        iov[0].iov_len  = chunk.headerLen;
        iov[1].iov_base = const_cast<uint8_t*>(chunk.payload);
        iov[1].iov_len  = static_cast<size_t>(chunk.payloadLen);

        struct msghdr msg{};
        msg.msg_iov    = iov;
        msg.msg_iovlen = 2;

        for (auto& [key, client] : clients_) {
            msg.msg_name    = &client.addr;
            msg.msg_namelen = sizeof(client.addr);
            if (::sendmsg(sock_, &msg, MSG_DONTWAIT) < 0 && errno != EAGAIN)
                logWarn(name_ + " sendmsg to " + key + ": " + strerror(errno));
        }
    }
}

void UdpRawServer::pushFrame(RawFrame frame) {
    queue_.push(std::move(frame));
}

int UdpRawServer::clientCount() const {
    std::lock_guard lock(clientsMu_);
    return static_cast<int>(clients_.size());
}

// ---------------------------------------------------------------------------
// Software YUV420 flip (same as original — hflip/vflip on raw buffer)
// ---------------------------------------------------------------------------

static void flipHorizontalPlane(uint8_t* plane, int width, int height, int stride) {
    for (int y = 0; y < height; ++y) {
        uint8_t* row = plane + y * stride;
        for (int x = 0; x < width / 2; ++x)
            std::swap(row[x], row[width - 1 - x]);
    }
}

static void flipVerticalPlane(uint8_t* plane, int height, int stride) {
    std::vector<uint8_t> tmp(static_cast<size_t>(stride));
    for (int y = 0; y < height / 2; ++y) {
        uint8_t* top = plane + y * stride;
        uint8_t* bot = plane + (height - 1 - y) * stride;
        memcpy(tmp.data(), top,        stride);
        memcpy(top,        bot,        stride);
        memcpy(bot,        tmp.data(), stride);
    }
}

static void flipYUV420(uint8_t* data, int width, int height, int stride,
                       bool hflip, bool vflip) {
    if (!hflip && !vflip) return;
    const int uvWidth  = width  / 2;
    const int uvHeight = height / 2;
    const int uvStride = stride / 2;
    const size_t yBytes  = static_cast<size_t>(stride * height);
    const size_t uvBytes = static_cast<size_t>(uvStride * uvHeight);
    uint8_t* yPlane = data;
    uint8_t* uPlane = data + yBytes;
    uint8_t* vPlane = data + yBytes + uvBytes;
    if (hflip) {
        flipHorizontalPlane(yPlane, width,   height,   stride);
        flipHorizontalPlane(uPlane, uvWidth, uvHeight, uvStride);
        flipHorizontalPlane(vPlane, uvWidth, uvHeight, uvStride);
    }
    if (vflip) {
        flipVerticalPlane(yPlane, height,   stride);
        flipVerticalPlane(uPlane, uvHeight, uvStride);
        flipVerticalPlane(vPlane, uvHeight, uvStride);
    }
}

// ---------------------------------------------------------------------------
// TelemetryServer (unchanged from original)
// ---------------------------------------------------------------------------

TelemetryServer::TelemetryServer(int port) : port_(port) {}

void TelemetryServer::start() {
    thread_ = std::thread(&TelemetryServer::acceptLoop, this);
    thread_.detach();
}

// Current UTC offset in minutes (east positive), read fresh each call so
// it tracks DST transitions automatically with no restart needed. Comes
// from this machine's own system timezone (tm_gmtoff, a glibc/BSD
// extension) — since picam-raw is the process actually reading
// CLOCK_REALTIME for each frame's timestamp_us, this is the
// authoritative answer to "what timezone was that timestamp captured
// in," rather than a consumer having to assume its own system timezone
// matches picam-raw's.
static int currentUtcOffsetMinutes() {
    std::time_t now = std::time(nullptr);
    std::tm local{};
    ::localtime_r(&now, &local);
    return static_cast<int>(local.tm_gmtoff / 60);
}

void TelemetryServer::broadcast(float lux, int activeCam) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(1);
    ss << "{\"lux\":" << lux
       << ",\"active_camera\":" << activeCam
       << ",\"camera_label\":\"" << g_config.cameras[activeCam].label << "\""
       << ",\"utc_offset_minutes\":" << currentUtcOffsetMinutes()
       << "}\n";
    std::string line = ss.str();
    std::lock_guard lock(clientsMu_);
    std::vector<int> alive;
    for (int fd : clients_) {
        if (::send(fd, line.data(), line.size(), MSG_NOSIGNAL) >= 0)
            alive.push_back(fd);
        else
            ::close(fd);
    }
    clients_ = std::move(alive);
}

void TelemetryServer::acceptLoop() {
    int srv = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt  = 1;
    ::setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(port_));
    ::bind(srv, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(srv, 10);
    logInfo("Telemetry server listening on :" + std::to_string(port_));
    while (true) {
        sockaddr_in client{};
        socklen_t   len = sizeof(client);
        int fd = ::accept(srv, reinterpret_cast<sockaddr*>(&client), &len);
        if (fd < 0) continue;
        char ipBuf[INET_ADDRSTRLEN];
        ::inet_ntop(AF_INET, &client.sin_addr, ipBuf, sizeof(ipBuf));
        logInfo("Telemetry client: " + std::string(ipBuf) + ":" +
                std::to_string(ntohs(client.sin_port)));
        std::lock_guard lock(clientsMu_);
        clients_.push_back(fd);
    }
}

// ---------------------------------------------------------------------------
// CommandServer (unchanged from original)
// ---------------------------------------------------------------------------

CommandServer::CommandServer(int port) : port_(port) {}

void CommandServer::start() {
    thread_ = std::thread(&CommandServer::acceptLoop, this);
    thread_.detach();
}

void CommandServer::acceptLoop() {
    int srv = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt  = 1;
    ::setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(port_));
    ::bind(srv, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(srv, 10);
    logInfo("Command server listening on :" + std::to_string(port_));
    while (true) {
        sockaddr_in client{};
        socklen_t   len = sizeof(client);
        int fd = ::accept(srv, reinterpret_cast<sockaddr*>(&client), &len);
        if (fd < 0) continue;
        std::thread(&CommandServer::handleClient, this, fd).detach();
    }
}

void CommandServer::handleClient(int fd) {
    char    buf[64] = {};
    ssize_t n       = ::recv(fd, buf, sizeof(buf) - 1, 0);
    if (n > 0) {
        std::string cmd(buf, n);
        cmd.erase(0, cmd.find_first_not_of(" \t\r\n"));
        auto last = cmd.find_last_not_of(" \t\r\n");
        if (last != std::string::npos) cmd.erase(last + 1);
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);

        if (cmd.substr(0, 6) == "switch") {
            std::string numStr = cmd.substr(6);
            if (numStr.empty() ||
                numStr.find_first_not_of("0123456789") != std::string::npos) {
                std::string err = "{\"ok\":false,\"error\":\"use switch0, switch1, ...\"}\n";
                ::send(fd, err.data(), err.size(), 0);
            } else {
                int idx = std::stoi(numStr);
                if (idx < 0 || idx >= static_cast<int>(g_config.cameras.size())) {
                    std::string err = "{\"ok\":false,\"error\":\"camera index " +
                                      std::to_string(idx) + " not configured\"}\n";
                    ::send(fd, err.data(), err.size(), 0);
                } else if (idx == g_config.activeCamera) {
                    std::string resp = "{\"ok\":true,\"active_camera\":" +
                                       std::to_string(idx) +
                                       ",\"note\":\"already active\"}\n";
                    ::send(fd, resp.data(), resp.size(), 0);
                } else {
                    logInfo("Switching to camera " + std::to_string(idx) +
                            " (" + g_config.cameras[static_cast<size_t>(idx)].label + ")");
                    if (g_switchCamera)
                        std::thread([idx]{ g_switchCamera(idx); }).detach();
                    std::string resp = "{\"ok\":true,\"active_camera\":" +
                                       std::to_string(idx) +
                                       ",\"label\":\"" +
                                       g_config.cameras[static_cast<size_t>(idx)].label +
                                       "\"}\n";
                    ::send(fd, resp.data(), resp.size(), 0);
                }
            }
        } else if (cmd == "status") {
            int   active;
            float lux;
            { std::lock_guard lock(g_state.mu); active = g_state.activeCamera; lux = g_state.lux; }
            std::ostringstream ss;
            ss << std::fixed << std::setprecision(1);
            ss << "{\"ok\":true"
               << ",\"active_camera\":" << active
               << ",\"label\":\"" << g_config.cameras[static_cast<size_t>(active)].label << "\""
               << ",\"num_cameras\":" << g_config.cameras.size()
               << ",\"lux_camera\":" << g_config.luxCamera
               << ",\"lux\":" << lux
               << ",\"cameras\":[";
            for (size_t i = 0; i < g_config.cameras.size(); ++i) {
                if (i > 0) ss << ",";
                ss << "{\"index\":" << i
                   << ",\"label\":\"" << g_config.cameras[i].label << "\""
                   << ",\"active\":" << (static_cast<int>(i) == active ? "true" : "false")
                   << "}";
            }
            ss << "]}\n";
            std::string resp = ss.str();
            ::send(fd, resp.data(), resp.size(), 0);
        } else {
            std::string err =
                "{\"ok\":false,\"error\":\"unknown command\","
                "\"commands\":[\"switch<n>\",\"status\"]}\n";
            ::send(fd, err.data(), err.size(), 0);
        }
    }
    ::close(fd);
}

// ---------------------------------------------------------------------------
// CameraImpl — one per physical camera
// ---------------------------------------------------------------------------

static void saveState(int activeCam);

struct CameraImpl {
    CameraConfig camCfg;
    bool         isActive = false;

    std::shared_ptr<UdpRawServer> mainServer;
    std::shared_ptr<UdpRawServer> loresServer;

    libcamera::CameraManager*                        camMgr = nullptr;
    std::shared_ptr<libcamera::Camera>               camera;
    std::unique_ptr<libcamera::CameraConfiguration>  camConfig;
    std::unique_ptr<libcamera::FrameBufferAllocator> allocator;
    libcamera::Stream*                               mainLibcam  = nullptr;
    libcamera::Stream*                               loresLibcam = nullptr;
    std::vector<std::unique_ptr<libcamera::Request>> requests;
    unsigned int                                     mainStride_  = 0;
    unsigned int                                     loresStride_ = 0;
    std::optional<libcamera::ControlList>            startControls_;
    std::atomic<bool>                                planeLayoutLogged_{false};

    // fps tracking
    std::atomic<int>                                 frameCount_{0};
    std::chrono::steady_clock::time_point            fpsWindow_{std::chrono::steady_clock::now()};

    // -----------------------------------------------------------------------
    bool setup() {
        if (!camMgr) { logError("CameraImpl: camMgr not set"); return false; }

        auto cameras = camMgr->cameras();
        if (static_cast<int>(cameras.size()) <= camCfg.index) {
            logError("Camera index " + std::to_string(camCfg.index) +
                     " not found (" + std::to_string(cameras.size()) + " detected)");
            return false;
        }

        camera = cameras[static_cast<size_t>(camCfg.index)];
        if (camera->acquire()) {
            logError("Failed to acquire camera " + std::to_string(camCfg.index));
            return false;
        }
        logInfo("Camera " + std::to_string(camCfg.index) + " acquired: " + camera->id());

        camConfig = camera->generateConfiguration({
            libcamera::StreamRole::VideoRecording,
            libcamera::StreamRole::Viewfinder
        });
        if (!camConfig || camConfig->size() < 2) {
            logError("Failed to generate dual-stream config for camera " +
                     std::to_string(camCfg.index));
            return false;
        }

        auto& mainCfg        = camConfig->at(0);
        mainCfg.size         = {static_cast<uint32_t>(camCfg.mainWidth),
                                 static_cast<uint32_t>(camCfg.mainHeight)};
        mainCfg.pixelFormat  = libcamera::formats::YUV420;
        mainCfg.bufferCount  = 4;

        auto& loresCfg       = camConfig->at(1);
        loresCfg.size        = {static_cast<uint32_t>(camCfg.loresWidth),
                                 static_cast<uint32_t>(camCfg.loresHeight)};
        loresCfg.pixelFormat = libcamera::formats::YUV420;
        loresCfg.bufferCount = 4;

        camConfig->orientation = libcamera::Orientation::Rotate0;

        auto vstatus = camConfig->validate();
        if (vstatus == libcamera::CameraConfiguration::Invalid) {
            logError("Camera " + std::to_string(camCfg.index) + " config invalid");
            return false;
        }
        if (vstatus == libcamera::CameraConfiguration::Adjusted)
            logInfo("Camera " + std::to_string(camCfg.index) + " config adjusted: " +
                    camConfig->at(0).toString() + " / " + camConfig->at(1).toString());

        if (camera->configure(camConfig.get())) {
            logError("Camera " + std::to_string(camCfg.index) + " configure failed");
            return false;
        }

        mainLibcam   = camConfig->at(0).stream();
        loresLibcam  = camConfig->at(1).stream();
        mainStride_  = camConfig->at(0).stride;
        loresStride_ = camConfig->at(1).stride;

        // validate() may have adjusted the requested sizes to whatever the
        // sensor/ISP actually supports (see the "config adjusted" log
        // above) — copyAndSend must use the size libcamera actually
        // configured, not the originally requested camCfg.mainWidth/
        // mainHeight, or every row gets read at the wrong offset.
        const auto& mainSize  = camConfig->at(0).size;
        const auto& loresSize = camConfig->at(1).size;
        if (static_cast<int>(mainSize.width) != camCfg.mainWidth ||
            static_cast<int>(mainSize.height) != camCfg.mainHeight)
            logInfo("Camera " + std::to_string(camCfg.index) +
                    " main stream resized by libcamera: requested " +
                    std::to_string(camCfg.mainWidth) + "x" + std::to_string(camCfg.mainHeight) +
                    ", actual " + std::to_string(mainSize.width) + "x" + std::to_string(mainSize.height));
        if (static_cast<int>(loresSize.width) != camCfg.loresWidth ||
            static_cast<int>(loresSize.height) != camCfg.loresHeight)
            logInfo("Camera " + std::to_string(camCfg.index) +
                    " lores stream resized by libcamera: requested " +
                    std::to_string(camCfg.loresWidth) + "x" + std::to_string(camCfg.loresHeight) +
                    ", actual " + std::to_string(loresSize.width) + "x" + std::to_string(loresSize.height));
        camCfg.mainWidth   = static_cast<int>(mainSize.width);
        camCfg.mainHeight  = static_cast<int>(mainSize.height);
        camCfg.loresWidth  = static_cast<int>(loresSize.width);
        camCfg.loresHeight = static_cast<int>(loresSize.height);

        logInfo("Camera " + std::to_string(camCfg.index) +
                " strides: main=" + std::to_string(mainStride_) +
                " lores=" + std::to_string(loresStride_));

        allocator = std::make_unique<libcamera::FrameBufferAllocator>(camera);
        for (auto* s : {mainLibcam, loresLibcam})
            if (allocator->allocate(s) < 0) {
                logError("Buffer allocation failed"); return false;
            }

        const auto& mainBufs  = allocator->buffers(mainLibcam);
        const auto& loresBufs = allocator->buffers(loresLibcam);
        size_t nReqs = std::min(mainBufs.size(), loresBufs.size());
        for (size_t i = 0; i < nReqs; ++i) {
            auto req = camera->createRequest();
            if (!req) { logError("createRequest failed"); return false; }
            req->addBuffer(mainLibcam,  mainBufs[i].get());
            req->addBuffer(loresLibcam, loresBufs[i].get());
            requests.push_back(std::move(req));
        }

        camera->requestCompleted.connect(this, &CameraImpl::onRequestCompleted);

        libcamera::ControlList ctrl(camera->controls());
        int64_t frameDur = 1'000'000LL / camCfg.framerate;
        ctrl.set(libcamera::controls::FrameDurationLimits,
                 libcamera::Span<const int64_t, 2>({frameDur, frameDur}));
        ctrl.set(libcamera::controls::AfMode,
                 libcamera::controls::AfModeContinuous);
        startControls_ = std::move(ctrl);
        return true;
    }

    // -----------------------------------------------------------------------
    void onRequestCompleted(libcamera::Request* request) {
        if (request->status() == libcamera::Request::RequestCancelled)
            return;

        // Log plane layout once
        if (!planeLayoutLogged_.exchange(true)) {
            for (auto* lcStream : {mainLibcam, loresLibcam}) {
                auto* fb = request->findBuffer(lcStream);
                if (!fb) continue;
                std::string lbl = (lcStream == mainLibcam) ? "main" : "lores";
                logInfo("Cam" + std::to_string(camCfg.index) +
                        " plane layout [" + lbl + "]:");
                for (size_t i = 0; i < fb->planes().size(); ++i) {
                    const auto& p = fb->planes()[i];
                    logInfo("  plane[" + std::to_string(i) + "] fd=" +
                            std::to_string(p.fd.get()) +
                            " offset=" + std::to_string(p.offset) +
                            " length=" + std::to_string(p.length));
                }
            }
        }

        const auto& meta = request->metadata();

        // Lux telemetry — only from the designated lux camera
        if (camCfg.index == g_config.luxCamera) {
            auto luxOpt = meta.get(libcamera::controls::Lux);
            if (luxOpt) {
                float lux = *luxOpt;
                { std::lock_guard lock(g_state.mu); g_state.lux = lux; }
                if (g_telemetry)
                    g_telemetry->broadcast(lux, g_config.activeCamera);
            }
        }

        // Video — only send when this camera is active
        if (!isActive) {
            request->reuse(libcamera::Request::ReuseBuffers);
            camera->queueRequest(request);
            return;
        }

        // Wall-clock timestamp in microseconds (used for OSD overlay)
        struct timespec ts{};
        ::clock_gettime(CLOCK_REALTIME, &ts);
        int64_t tsUs = ts.tv_sec * 1000000LL + ts.tv_nsec / 1000LL;

        // Log actual camera fps every 30 seconds
        {
            int count = ++frameCount_;
            auto now  = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - fpsWindow_).count();
            if (elapsed >= 30000) {
                float fps = count * 1000.0f / static_cast<float>(elapsed);
                logInfo("cam" + std::to_string(camCfg.index) +
                        " camera fps=" + [&]{
                            std::ostringstream s;
                            s << std::fixed << std::setprecision(1) << fps;
                            return s.str();
                        }() +
                        (isActive ? " [active]" : " [standby]"));
                frameCount_ = 0;
                fpsWindow_  = now;
            }
        }

        // Copy one stream's DMA buffer to a RawFrame and push to the server.
        //
        // libcamera pads each captured row up to `stride` bytes, which can
        // exceed the logical `w` (alignment requirements differ per stream
        // — e.g. the unscaled main tap vs. the ISP-resized lores tap — so
        // stride == w is not guaranteed for either). The wire protocol
        // (see file header / README) has no stride field and promises
        // tightly packed width*height planes, so padding must be stripped
        // here, row by row, rather than bulk-copying stride*height bytes:
        // shipping the padding as if it were pixel data shifts every row
        // after the first, producing diagonal tearing on the client.
        auto copyAndSend = [&](libcamera::Stream*    lcStream,
                               int                   w, int h, int stride,
                               UdpRawServer*         server) {
            auto* fb = request->findBuffer(lcStream);
            if (!fb || server->clientCount() == 0) return;

            const auto& planes = fb->planes();
            if (planes.empty()) return;

            size_t mapSize = 0;
            for (const auto& p : planes)
                mapSize = std::max(mapSize,
                                   static_cast<size_t>(p.offset) + p.length);

            void* base = ::mmap(nullptr, mapSize, PROT_READ, MAP_SHARED,
                                planes[0].fd.get(), 0);
            if (base == MAP_FAILED) return;

            const uint8_t* buf      = static_cast<const uint8_t*>(base);
            const int      uvStride = stride / 2;
            const int      uvWidth  = w / 2;
            const int      uvHeight = h / 2;
            const size_t   yBytes   = static_cast<size_t>(w) * h;
            const size_t   uvBytes  = static_cast<size_t>(uvWidth) * uvHeight;

            RawFrame rf;
            rf.width       = w;
            rf.height      = h;
            rf.stride      = w;
            rf.timestampUs = tsUs;
            rf.cameraIndex = camCfg.index;
            rf.cameraLabel = camCfg.label;
            rf.data.resize(yBytes + uvBytes * 2, 128);

            auto copyPlane = [&](size_t srcOffset, size_t srcLen,
                                  uint8_t* dst, int rowBytes, int rows,
                                  int srcStride) {
                for (int y = 0; y < rows; ++y) {
                    size_t srcRowOff = srcOffset + static_cast<size_t>(y) * srcStride;
                    if (srcRowOff + static_cast<size_t>(rowBytes) > srcOffset + srcLen)
                        break;
                    memcpy(dst + static_cast<size_t>(y) * rowBytes,
                           buf + srcRowOff, rowBytes);
                }
            };

            // Copy Y, U, V planes, stripping stride padding to `w`/`uvWidth`
            if (planes.size() > 0)
                copyPlane(planes[0].offset, planes[0].length,
                          rf.data.data(), w, h, stride);
            if (planes.size() > 1)
                copyPlane(planes[1].offset, planes[1].length,
                          rf.data.data() + yBytes, uvWidth, uvHeight, uvStride);
            if (planes.size() > 2)
                copyPlane(planes[2].offset, planes[2].length,
                          rf.data.data() + yBytes + uvBytes, uvWidth, uvHeight, uvStride);

            ::munmap(base, mapSize);

            // Apply software flip
            flipYUV420(rf.data.data(), rf.width, rf.height, rf.stride,
                       camCfg.hflip, camCfg.vflip);

            server->pushFrame(std::move(rf));
        };

        copyAndSend(mainLibcam,
                    camCfg.mainWidth,  camCfg.mainHeight,
                    static_cast<int>(mainStride_),
                    mainServer.get());
        copyAndSend(loresLibcam,
                    camCfg.loresWidth, camCfg.loresHeight,
                    static_cast<int>(loresStride_),
                    loresServer.get());

        request->reuse(libcamera::Request::ReuseBuffers);
        camera->queueRequest(request);
    }

    // -----------------------------------------------------------------------
    bool start() {
        if (!setup()) return false;
        if (camera->start(&startControls_.value())) {
            logError("camera->start() failed for camera " +
                     std::to_string(camCfg.index));
            return false;
        }
        for (auto& req : requests)
            camera->queueRequest(req.get());
        logInfo("Camera " + std::to_string(camCfg.index) +
                " (" + camCfg.label + ") running" +
                (isActive ? " [ACTIVE]" : " [standby]"));
        return true;
    }

    void stop() {
        if (camera) camera->stop();
        requests.clear();
        allocator.reset();
        if (camera) { camera->release(); camera.reset(); }
        startControls_.reset();
        mainStride_ = 0; loresStride_ = 0;
        planeLayoutLogged_ = false;
    }
};

// ---------------------------------------------------------------------------
// CameraManager::Impl
// ---------------------------------------------------------------------------

struct CameraManager::Impl {
    std::shared_ptr<UdpRawServer>  mainServer;
    std::shared_ptr<UdpRawServer>  loresServer;
    std::vector<std::unique_ptr<CameraImpl>> cams;
    std::unique_ptr<libcamera::CameraManager> lcamMgr;

    bool start() {
        lcamMgr = std::make_unique<libcamera::CameraManager>();
        if (lcamMgr->start()) {
            logError("Failed to start libcamera CameraManager");
            return false;
        }
        logInfo("libcamera CameraManager started — " +
                std::to_string(lcamMgr->cameras().size()) + " camera(s) detected");

        for (int i = 0; i < static_cast<int>(g_config.cameras.size()); ++i) {
            auto cam = std::make_unique<CameraImpl>();
            cam->camCfg      = g_config.cameras[static_cast<size_t>(i)];
            cam->isActive    = (i == g_config.activeCamera);
            cam->mainServer  = mainServer;
            cam->loresServer = loresServer;
            cam->camMgr      = lcamMgr.get();
            if (!cam->start()) {
                logError("Failed to start camera " + std::to_string(i));
                for (auto& c : cams) c->stop();
                lcamMgr->stop();
                return false;
            }
            cams.push_back(std::move(cam));
        }
        return true;
    }

    void stop() {
        for (auto& cam : cams) cam->stop();
        cams.clear();
        if (lcamMgr) { lcamMgr->stop(); lcamMgr.reset(); }
    }

    void switchTo(int idx) {
        if (idx < 0 || idx >= static_cast<int>(cams.size())) return;
        if (idx == g_config.activeCamera) return;

        logInfo("Switching camera " + std::to_string(g_config.activeCamera) +
                " -> " + std::to_string(idx) +
                " (" + g_config.cameras[static_cast<size_t>(idx)].label + ")");

        cams[static_cast<size_t>(g_config.activeCamera)]->isActive = false;
        cams[static_cast<size_t>(idx)]->isActive                   = true;

        g_config.activeCamera = idx;
        { std::lock_guard lock(g_state.mu); g_state.activeCamera = idx; }
        saveState(idx);

        logInfo("Camera switch complete → " +
                g_config.cameras[static_cast<size_t>(idx)].label + " [ACTIVE]");
    }
};

CameraManager::CameraManager(std::shared_ptr<UdpRawServer> mainServer,
                             std::shared_ptr<UdpRawServer> loresServer)
    : impl_(std::make_unique<Impl>()),
      mainServer_(std::move(mainServer)),
      loresServer_(std::move(loresServer))
{
    impl_->mainServer  = mainServer_;
    impl_->loresServer = loresServer_;
}

CameraManager::~CameraManager() { stop(); }
bool CameraManager::start()          { return impl_->start(); }
void CameraManager::stop()           { impl_->stop(); }
void CameraManager::switchTo(int idx){ impl_->switchTo(idx); }

// ---------------------------------------------------------------------------
// Config loading (adapted from original)
// ---------------------------------------------------------------------------

static std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

static bool parseBool(const std::string& v) {
    std::string l = v;
    std::transform(l.begin(), l.end(), l.begin(), ::tolower);
    return l == "true" || l == "1" || l == "yes";
}

// Baseline main/lores dimensions for newly-constructed CameraConfig
// entries, overwritten by loadSharedStreamConfig() below before any
// camera gets constructed. Kept as file-scope state (rather than
// threaded through function signatures) since loadConfigFile()'s
// per-camera-section default construction needs them too.
static int g_sharedMainWidth   = 2304;
static int g_sharedMainHeight  = 1296;
static int g_sharedLoresWidth  = 640;
static int g_sharedLoresHeight = 360;

// Reads /etc/aipicam/streams.conf's [stream] section — the single
// source of truth shared with picam-orchestrator/picam-hailo/
// picam-recorder, since picam-raw's wire protocol carries no
// width/height field and every reader must already agree with what's
// actually sent. Applied before picam_raw.conf and CLI args are
// parsed, so both can still explicitly override for local debugging,
// but a stock install with an unmodified picam_raw.conf always
// reflects this shared file instead of a second, independently
// hand-copied default that can silently drift from it.
static void loadSharedStreamConfig() {
    const std::string path = "/etc/aipicam/streams.conf";
    std::ifstream f(path);
    if (!f.is_open()) {
        logInfo("Shared stream config not found: " + path + " — using compiled-in defaults");
        return;
    }
    logInfo("Loading shared stream config from: " + path);
    std::string line, section;
    while (std::getline(f, line)) {
        auto hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);
        line = trim(line);
        if (line.empty()) continue;
        if (line.front() == '[' && line.back() == ']') {
            section = trim(line.substr(1, line.size() - 2));
            continue;
        }
        if (section != "stream") continue;
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = trim(line.substr(0, eq));
        std::string val = trim(line.substr(eq + 1));
        if (key.empty() || val.empty()) continue;

        if      (key == "main_width")     g_sharedMainWidth      = std::stoi(val);
        else if (key == "main_height")    g_sharedMainHeight     = std::stoi(val);
        else if (key == "lores_width")    g_sharedLoresWidth     = std::stoi(val);
        else if (key == "lores_height")   g_sharedLoresHeight    = std::stoi(val);
        else if (key == "main_port")      g_config.rawMainPort   = std::stoi(val);
        else if (key == "lores_port")     g_config.rawLoresPort  = std::stoi(val);
        else if (key == "telemetry_port") g_config.telemetryPort = std::stoi(val);
        else if (key == "command_port")   g_config.commandPort   = std::stoi(val);
    }
}

static void applyCameraKV(CameraConfig& c, const std::string& rawKey,
                           const std::string& rawVal) {
    std::string key = trim(rawKey);
    std::string val = trim(rawVal);
    auto hash = val.find('#');
    if (hash != std::string::npos) val = trim(val.substr(0, hash));
    if (key.empty() || val.empty()) return;
    std::replace(key.begin(), key.end(), '-', '_');
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);

    if      (key == "index")        c.index       = std::stoi(val);
    else if (key == "label")        c.label       = val;
    else if (key == "main_width")   c.mainWidth   = std::stoi(val);
    else if (key == "main_height")  c.mainHeight  = std::stoi(val);
    else if (key == "lores_width")  c.loresWidth  = std::stoi(val);
    else if (key == "lores_height") c.loresHeight = std::stoi(val);
    else if (key == "framerate")    c.framerate   = std::stoi(val);
    else if (key == "hflip")        c.hflip       = parseBool(val);
    else if (key == "vflip")        c.vflip       = parseBool(val);
}

static void applyGlobalKV(const std::string& rawKey, const std::string& rawVal) {
    std::string key = trim(rawKey);
    std::string val = trim(rawVal);
    auto hash = val.find('#');
    if (hash != std::string::npos) val = trim(val.substr(0, hash));
    if (key.empty() || val.empty()) return;
    std::replace(key.begin(), key.end(), '-', '_');
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);

    if      (key == "raw_main_port")   g_config.rawMainPort   = std::stoi(val);
    else if (key == "raw_lores_port")  g_config.rawLoresPort  = std::stoi(val);
    else if (key == "telemetry_port")  g_config.telemetryPort = std::stoi(val);
    else if (key == "command_port")    g_config.commandPort   = std::stoi(val);
    else if (key == "udp_chunk_size")  g_config.udpChunkSize  = std::stoi(val);
    else if (key == "client_timeout")  g_config.clientTimeoutS= std::stoi(val);
    else if (key == "cpu_core")        g_config.cpuCore       = std::stoi(val);
    else if (key == "lux_camera")      g_config.luxCamera     = std::stoi(val);
    else if (key == "active_camera")   g_config.activeCamera  = std::stoi(val);
    else if (key == "state_file")      g_config.stateFile     = val;
}

static int parseCameraSection(const std::string& line) {
    if (line.empty() || line.front() != '[' || line.back() != ']') return -1;
    std::string inner = line.substr(1, line.size() - 2);
    std::string prefix;
    if (inner.substr(0, 6) == "camera") prefix = inner.substr(6);
    else if (inner.substr(0, 3) == "cam") prefix = inner.substr(3);
    else return -1;
    if (prefix.empty() || prefix.find_first_not_of("0123456789") != std::string::npos)
        return -1;
    return std::stoi(prefix);
}

static void loadConfigFile(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open()) {
        logInfo("Config file not found: " + path + " — using defaults");
        return;
    }
    logInfo("Loading config from: " + path);
    std::map<int, CameraConfig> camMap;
    std::string line;
    int section = -1;
    while (std::getline(f, line)) {
        auto hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);
        line = trim(line);
        if (line.empty()) continue;
        if (line.front() == '[') {
            int camIdx = parseCameraSection(line);
            if (camIdx >= 0) {
                section = camIdx;
                if (camMap.find(camIdx) == camMap.end()) {
                    CameraConfig def; def.index = camIdx;
                    def.label = "cam" + std::to_string(camIdx);
                    def.mainWidth   = g_sharedMainWidth;
                    def.mainHeight  = g_sharedMainHeight;
                    def.loresWidth  = g_sharedLoresWidth;
                    def.loresHeight = g_sharedLoresHeight;
                    camMap[camIdx] = def;
                }
            } else { section = -1; }
        } else {
            auto eq = line.find('=');
            if (eq == std::string::npos) continue;
            if (section >= 0)
                applyCameraKV(camMap[section], line.substr(0, eq), line.substr(eq+1));
            else
                applyGlobalKV(line.substr(0, eq), line.substr(eq+1));
        }
    }
    if (!camMap.empty()) {
        g_config.cameras.clear();
        for (auto& [idx, cfg] : camMap)
            g_config.cameras.push_back(cfg);
    }
}

static int loadState() {
    if (g_config.stateFile.empty()) return -1;
    std::ifstream f(g_config.stateFile);
    if (!f.is_open()) return -1;
    std::string line;
    if (!std::getline(f, line)) return -1;
    line = trim(line);
    if (line.empty() || line.find_first_not_of("0123456789") != std::string::npos)
        return -1;
    return std::stoi(line);
}

static void saveState(int activeCam) {
    if (g_config.stateFile.empty()) return;
    std::string dir = g_config.stateFile;
    auto slash = dir.rfind('/');
    if (slash != std::string::npos) {
        dir = dir.substr(0, slash);
        ::system(("mkdir -p " + dir + " 2>/dev/null").c_str());
    }
    std::ofstream f(g_config.stateFile, std::ios::trunc);
    if (!f.is_open()) { logWarn("Could not write state file: " + g_config.stateFile); return; }
    f << activeCam << "\n";
}

static void printUsage(const char* prog) {
    std::cerr <<
        "Usage: " << prog << " [--config FILE] [OPTION...]\n"
        "\n"
        "Stream ports and main/lores dimensions default from the shared\n"
        "/etc/aipicam/streams.conf (see the aipicam-config package) — the\n"
        "single source of truth other camera-pipeline services also read,\n"
        "since the UDP wire protocol carries no width/height field. The\n"
        "options below still override it, for local debugging.\n"
        "\n"
        "Config file sections:\n"
        "  [network]             — global settings\n"
        "  [camera0]             — first camera\n"
        "  [cameraN]             — any number of cameras\n"
        "\n"
        "Global options:\n"
        "  --config PATH         Config file (default: picam_raw.conf)\n"
        "  --raw-main-port N     UDP port for main stream   (default: 8560)\n"
        "  --raw-lores-port N    UDP port for lores stream  (default: 8561)\n"
        "  --telemetry-port N    Telemetry port             (default: 8555)\n"
        "  --command-port N      Command port               (default: 8556)\n"
        "  --udp-chunk-size N    Bytes per UDP packet       (default: 60000)\n"
        "  --client-timeout N    Client idle timeout secs   (default: 10)\n"
        "  --active-camera N     Start on camera N          (default: 0)\n"
        "  --lux-camera N        Camera index to read lux from\n"
        "  --cpu-core N          CPU core to pin process to (default: 0)\n"
        "  --state-file PATH     File to persist last active camera\n"
        "\n"
        "Per-camera options:\n"
        "  --main-width/height   Main stream resolution\n"
        "  --lores-width/height  Lores stream resolution\n"
        "  --framerate N\n"
        "  --hflip/vflip true|false\n"
        "\n"
        "Client registration:\n"
        "  Send any UDP datagram to the server port to register as a client.\n"
        "  You will receive frames until " +
        std::to_string(g_config.clientTimeoutS) + "s of silence.\n"
        "  Re-send any datagram to refresh the timeout.\n"
        "\n"
        "Frame reassembly:\n"
        "  Each packet: [frame_seq:4][chunk_seq:2][total_chunks:2][yuv420...]\n"
        "  All integers big-endian. Collect total_chunks packets with the same\n"
        "  frame_seq, sort by chunk_seq, concatenate payloads → one YUV420 frame.\n";
}

bool loadConfig(int argc, char* argv[]) {
    loadSharedStreamConfig();

    if (g_config.cameras.empty()) {
        CameraConfig def; def.index = 0; def.label = "cam0";
        def.mainWidth   = g_sharedMainWidth;
        def.mainHeight  = g_sharedMainHeight;
        def.loresWidth  = g_sharedLoresWidth;
        def.loresHeight = g_sharedLoresHeight;
        g_config.cameras.push_back(def);
    }
    std::string configPath = "picam_raw.conf";
    for (int i = 1; i < argc - 1; ++i) {
        if (std::string(argv[i]) == "--config") { configPath = argv[i+1]; break; }
        if (std::string(argv[i]) == "--help" || std::string(argv[i]) == "-h") {
            printUsage(argv[0]); return false;
        }
    }
    loadConfigFile(configPath);

    bool explicitActive = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") { printUsage(argv[0]); return false; }
        if (arg == "--config") { ++i; continue; }
        if (arg == "--active-camera" && i+1 < argc) {
            g_config.activeCamera = std::stoi(argv[++i]);
            explicitActive = true; continue;
        }
        if (arg.substr(0,2) == "--" && i+1 < argc) {
            std::string key = arg.substr(2);
            std::string val = argv[++i];
            applyGlobalKV(key, val);
            if (!g_config.cameras.empty())
                applyCameraKV(g_config.cameras[static_cast<size_t>(g_config.activeCamera)],
                              key, val);
        }
    }

    if (g_config.cameras.empty()) { logError("No cameras configured"); return false; }
    int n = static_cast<int>(g_config.cameras.size());
    if (g_config.activeCamera < 0 || g_config.activeCamera >= n) {
        logWarn("active_camera out of range, clamping to 0");
        g_config.activeCamera = 0;
    }
    if (g_config.luxCamera < 0 || g_config.luxCamera >= n) {
        logWarn("lux_camera out of range, clamping to 0");
        g_config.luxCamera = 0;
    }
    if (!explicitActive) {
        int saved = loadState();
        if (saved >= 0 && saved < n) {
            g_config.activeCamera = saved;
            logInfo("Restored active camera " + std::to_string(saved) + " from state file");
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Signal handler + main
// ---------------------------------------------------------------------------

static std::atomic<bool> g_shutdown{false};
static void sigHandler(int) { g_shutdown = true; }

int main(int argc, char* argv[]) {
    logInfo("picam-raw starting");

    if (!loadConfig(argc, argv))
        return 1;

    // Pin to isolated RT core
    {
        cpu_set_t cpus;
        CPU_ZERO(&cpus);
        CPU_SET(static_cast<size_t>(g_config.cpuCore), &cpus);
        if (::sched_setaffinity(0, sizeof(cpus), &cpus) != 0)
            logWarn("Failed to set CPU affinity: " + std::string(strerror(errno)));
        else
            logInfo("Pinned to CPU core " + std::to_string(g_config.cpuCore));
    }

    // Real-time scheduling
    {
        struct sched_param sp{};
        sp.sched_priority = 99;
        if (::sched_setscheduler(0, SCHED_FIFO, &sp) == 0) {
            logInfo("Scheduling: SCHED_FIFO priority 99");
        } else {
            logWarn("Could not set SCHED_FIFO — falling back to nice -20");
            if (::setpriority(PRIO_PROCESS, 0, -20) != 0)
                logWarn("Could not set nice -20: " + std::string(strerror(errno)));
        }
    }

    // Lock memory
    if (::mlockall(MCL_CURRENT | MCL_FUTURE) == 0)
        logInfo("Memory locked (mlockall)");
    else
        logWarn("mlockall failed: " + std::string(strerror(errno)));

    const auto& sc = g_config.cameras[g_config.activeCamera];
    logInfo("Config: camera=" + std::to_string(g_config.activeCamera) +
            " (" + sc.label + ")" +
            " main=" + std::to_string(sc.mainWidth) + "x" +
            std::to_string(sc.mainHeight) +
            " lores=" + std::to_string(sc.loresWidth) + "x" +
            std::to_string(sc.loresHeight) +
            " @" + std::to_string(sc.framerate) + "fps");

    signal(SIGINT,  sigHandler);
    signal(SIGTERM, sigHandler);

    // Frame size info
    auto mainFrameBytes  = sc.mainWidth  * sc.mainHeight  * 3 / 2;
    auto loresFrameBytes = sc.loresWidth * sc.loresHeight * 3 / 2;
    auto mainChunks      = (mainFrameBytes  + g_config.udpChunkSize - 1) / g_config.udpChunkSize;
    auto loresChunks     = (loresFrameBytes + g_config.udpChunkSize - 1) / g_config.udpChunkSize;

    logInfo("Frame sizes:");
    logInfo("  main  " + std::to_string(sc.mainWidth)  + "x" +
            std::to_string(sc.mainHeight)  + " = " +
            std::to_string(mainFrameBytes/1024)  + " KB → " +
            std::to_string(mainChunks)  + " UDP packets/frame");
    logInfo("  lores " + std::to_string(sc.loresWidth) + "x" +
            std::to_string(sc.loresHeight) + " = " +
            std::to_string(loresFrameBytes/1024) + " KB → " +
            std::to_string(loresChunks) + " UDP packets/frame");

    auto mainServer  = std::make_shared<UdpRawServer>(
        g_config.rawMainPort,  g_config.udpChunkSize,
        g_config.clientTimeoutS, "main");
    auto loresServer = std::make_shared<UdpRawServer>(
        g_config.rawLoresPort, g_config.udpChunkSize,
        g_config.clientTimeoutS, "lores");

    TelemetryServer telemetry(g_config.telemetryPort);
    g_telemetry = &telemetry;
    telemetry.start();

    CommandServer commands(g_config.commandPort);
    commands.start();

    mainServer->start();
    loresServer->start();

    CameraManager cam(mainServer, loresServer);
    g_switchCamera = [&cam](int idx) { cam.switchTo(idx); };

    if (!cam.start()) {
        logError("Failed to start camera — aborting");
        return 1;
    }

    std::string ip = localIp();
    logInfo("Streaming raw YUV420:");
    logInfo("  main  UDP :" + std::to_string(g_config.rawMainPort)  +
            "  ← send any datagram to register");
    logInfo("  lores UDP :" + std::to_string(g_config.rawLoresPort) +
            "  ← send any datagram to register");
    logInfo("Telemetry: nc " + ip + " " + std::to_string(g_config.telemetryPort));
    logInfo("Commands:  echo switch<n>|status | nc " +
            ip + " " + std::to_string(g_config.commandPort));
    logInfo("Receiver example (lores):");
    logInfo("  python3 recv_raw.py --host " + ip +
            " --port " + std::to_string(g_config.rawLoresPort) +
            " --width " + std::to_string(sc.loresWidth) +
            " --height " + std::to_string(sc.loresHeight));

    while (!g_shutdown)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    logInfo("Shutting down...");
    cam.stop();
    return 0;
}

// ---------------------------------------------------------------------------
// CMakeLists.txt
// Save as CMakeLists.txt alongside picam_raw.hpp and picam_raw.cpp, then:
//   cmake -B build && cmake --build build -j4
//
// cmake_minimum_required(VERSION 3.16)
// project(picam_raw CXX)
//
// set(CMAKE_CXX_STANDARD 20)
// set(CMAKE_CXX_STANDARD_REQUIRED ON)
//
// find_package(PkgConfig REQUIRED)
// pkg_check_modules(LIBCAMERA REQUIRED libcamera)
//
// add_executable(picam_raw picam_raw.cpp)
//
// target_include_directories(picam_raw PRIVATE ${LIBCAMERA_INCLUDE_DIRS})
// target_link_libraries(picam_raw PRIVATE ${LIBCAMERA_LIBRARIES} pthread)
// target_compile_options(picam_raw PRIVATE ${LIBCAMERA_CFLAGS_OTHER} -Wall -Wextra -O2)
//
// install(TARGETS picam_raw DESTINATION bin)
