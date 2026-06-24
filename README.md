# picam-raw

Real-time raw YUV420 UDP streaming server for Raspberry Pi 5 with Pi Camera 3.

Replaces a traditional RTSP/H.264 pipeline with an uncompressed, low-latency stream
designed for applications where decoding delay is unacceptable — computer vision,
frame-synchronised overlays, or any pipeline that processes pixels directly.

---

## Streams at a glance

| Stream    | Protocol | Default port | Content                        |
|-----------|----------|-------------|--------------------------------|
| Main      | UDP      | 8560        | Full-res YUV420 (2304 × 1296) |
| Lores     | UDP      | 8561        | Scaled YUV420 (640 × 360)     |
| Telemetry | TCP      | 8555        | JSON: lux, active camera       |
| Command   | TCP      | 8556        | Camera switching, status       |

Both video streams use the same chunked-UDP protocol described below.
The server supports multiple simultaneous clients on each stream.

---

## How stream publishing works

### Client registration

There is no handshake. A client registers itself by sending **any UDP datagram**
to the stream port. The server records the sender's IP and port, and begins
forwarding frames immediately. Clients that do not send a datagram within
`client_timeout` seconds (default 10 s) are silently removed.

A keep-alive ping every ~5 seconds is enough to stay registered. The reference
client (`recv_raw.py`) sends a 1-byte ping on this schedule.

### Frame chunking

YUV420 frames are large (≈ 4.5 MB at 2304 × 1296). A single frame is split into
fixed-size UDP packets called **chunks**. Each chunk carries a small header
followed by raw pixel data.

**Chunk 0** (first packet of a frame) — 32-byte header:

| Offset | Size | Field           | Description                          |
|--------|------|-----------------|--------------------------------------|
| 0      | 4    | `frame_seq`     | Monotonically increasing frame number |
| 4      | 4    | `chunk_seq`     | Always 0 for chunk 0                 |
| 8      | 4    | `total_chunks`  | Total chunks in this frame           |
| 12     | 8    | `timestamp_us`  | CLOCK_REALTIME capture time (µs)     |
| 20     | 1    | `camera_index`  | Which physical camera (0, 1, …)      |
| 21     | 15   | `camera_label`  | NUL-terminated ASCII label           |

**Chunks 1 … N** — 8-byte header:

| Offset | Size | Field          | Description             |
|--------|------|----------------|-------------------------|
| 0      | 4    | `frame_seq`    | Same as chunk 0         |
| 4      | 4    | `chunk_seq`    | 1, 2, 3, …              |
| 8      | 4    | `total_chunks` | Same as chunk 0         |

The payload immediately follows the header in every chunk.

### Reassembly

Buffer chunks by `frame_seq`. Once all `total_chunks` chunks for a given
`frame_seq` have arrived, concatenate payloads in `chunk_seq` order to obtain
the raw YUV420 (I420 planar) frame:

```
Y plane  : width × height bytes
U plane  : (width/2) × (height/2) bytes
V plane  : (width/2) × (height/2) bytes
```

Drop any `frame_seq` for which a prior `frame_seq` has already been completed
(packets arrive in roughly frame order; the server does not retransmit).

### Telemetry (TCP :8555)

After connecting, the server pushes newline-terminated JSON once per second:

```json
{"lux": 312.5, "active_camera": 0}
```

### Command interface (TCP :8556)

Send one of the following newline-terminated commands:

| Command    | Effect                                      |
|------------|---------------------------------------------|
| `switch0`  | Activate camera 0 (stops streaming camera 1) |
| `switch1`  | Activate camera 1                           |
| `status`   | Returns a JSON status line                  |

The active camera is persisted across restarts (state file at
`/var/lib/picam-raw/state` by default).

---

## Reference client

`recv_raw.py` is a Python/OpenCV receiver that shows both streams in a window.

```bash
python3 recv_raw.py --host 192.168.1.10
```

It registers with the server, reassembles chunks, converts I420 → BGR, and
displays the result with `cv2.imshow`. Press `q` to quit.

Required packages: `opencv-python`, `numpy`.

---

## Building

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4
```

Run directly (skips RT scheduling if not root):

```bash
./build/picam_raw --config picam_raw.conf
```

### Debian package

```bash
dpkg-buildpackage -us -uc -b
```

The package installs:

| Path                                    | Contents                   |
|-----------------------------------------|----------------------------|
| `/usr/bin/picam_raw`                    | Server binary              |
| `/etc/picam-raw/picam_raw.conf`         | Configuration              |
| `/usr/lib/systemd/system/picam-raw.service` | systemd unit           |
| `/usr/share/picam-raw/recv_raw.py`      | Reference client           |
| `/var/lib/picam-raw/`                   | State directory            |

**Dependencies**: `libcamera-dev`, `pthread`. Recommends: `pi-block-cpu-cores`.

---

## Configuration (`picam_raw.conf`)

```ini
[network]
raw_main_port  = 8560          # UDP port for full-resolution stream
raw_lores_port = 8561          # UDP port for low-resolution stream
telemetry_port = 8555          # TCP port for lux/camera JSON
command_port   = 8556          # TCP port for switch/status commands
cpu_core       = 1             # CPU core to pin the process to (see RT setup)
active_camera  = 0             # Camera active at startup
lux_camera     = 0             # Camera used for lux readings
udp_chunk_size = 60000         # Payload bytes per UDP packet
client_timeout = 10            # Seconds before an idle client is removed
state_file     = /var/lib/picam-raw/state

[camera0]
index      = 0
label      = front
main_width  = 2304
main_height = 1296
lores_width  = 640
lores_height = 360
framerate  = 30
hflip      = true
vflip      = true

# [camera1]   — add a second section for multi-camera setups
```

Override any value at runtime:

```bash
picam_raw --config /etc/picam-raw/picam_raw.conf --raw_main_port 9000
```

---

## Real-time kernel setup (Raspberry Pi 5)

For sub-50 µs jitter, isolate cores from the Linux scheduler before running
the service.

### 1. Edit `/boot/firmware/cmdline.txt`

Append to the **single existing line** (do not add a newline):

```
isolcpus=1,2,3 nohz_full=1,2,3 rcu_nocbs=1,2,3 irqaffinity=0
```

| Parameter          | Effect                                                  |
|--------------------|---------------------------------------------------------|
| `isolcpus=1,2,3`  | Removes cores 1–3 from the kernel scheduler             |
| `nohz_full=1,2,3` | Disables the 1000 Hz scheduling tick on those cores     |
| `rcu_nocbs=1,2,3` | Moves RCU callbacks (a kernel sync mechanism) off those cores |
| `irqaffinity=0`   | Routes all hardware IRQs to core 0                      |

After reboot, set `cpu_core = 1` in `picam_raw.conf` (already the default).

### 2. Verify isolation

```bash
cat /sys/devices/system/cpu/isolated
# expected: 1-3

ps -eo pid,psr,comm | awk '$2==1'
# should show only picam_raw and kernel migration threads
```

### 3. Measure latency

```bash
sudo apt install rt-tests
taskset -c 1 cyclictest -p 99 -t 1 -n -i 1000 -l 10000
# Max latency should be <50 µs consistently
```

### 4. Force IRQ affinity at runtime (optional)

Some IRQs ignore `irqaffinity=`. Pin them manually:

```bash
for irq in /proc/irq/*/smp_affinity; do
  echo 1 | sudo tee "$irq" > /dev/null 2>&1
done
```

Add this to a `@reboot` cron job or a systemd unit ordered before `picam-raw`.

---

## systemd service

```bash
sudo systemctl enable --now picam-raw
sudo journalctl -u picam-raw -f
```

The service runs as the `picam-raw` system user with `CAP_SYS_NICE` (SCHED_FIFO)
and `CAP_IPC_LOCK` (mlockall). It restarts automatically on failure.
