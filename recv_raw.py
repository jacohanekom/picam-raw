#!/usr/bin/env python3
"""
recv_raw.py — receive raw YUV420 frames from picam_raw and display with OpenCV.

Usage:
    python3 recv_raw.py --host 192.168.1.x --port 8561 --width 640 --height 360

The script sends a registration datagram to the server, then reassembles
incoming chunks into full YUV420 frames and converts to BGR for display.

Packet header (8 bytes, big-endian):
    uint32  frame_seq
    uint16  chunk_seq
    uint16  total_chunks
"""

import argparse
import socket
import struct
import threading
import time
from collections import defaultdict

import cv2
import numpy as np

HEADER = struct.Struct(">IHH")   # frame_seq, chunk_seq, total_chunks
HEADER_SIZE = HEADER.size        # 8 bytes


def parse_args():
    p = argparse.ArgumentParser(description="Receive raw YUV420 UDP stream from picam_raw")
    p.add_argument("--host",   required=True,  help="Pi IP address")
    p.add_argument("--port",   type=int, default=8561, help="UDP port (default 8561 = lores)")
    p.add_argument("--width",  type=int, default=640)
    p.add_argument("--height", type=int, default=360)
    p.add_argument("--bind-port", type=int, default=0,
                   help="Local UDP port to bind (0 = OS assigns)")
    p.add_argument("--timeout", type=float, default=5.0,
                   help="Seconds between re-registration pings")
    p.add_argument("--save", metavar="PATH",
                   help="Write the first decoded frame to PATH (e.g. frame.png) "
                        "and exit, instead of opening a display window. Use this "
                        "on a headless Pi with no display attached.")
    return p.parse_args()


def register_loop(sock, dest, interval):
    """Periodically poke the server so it keeps us registered."""
    while True:
        try:
            sock.sendto(b"HELLO", dest)
        except Exception:
            pass
        time.sleep(interval)


def main():
    args = parse_args()
    dest = (args.host, args.port)
    expected_bytes = args.width * args.height * 3 // 2

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", args.bind_port))
    sock.settimeout(2.0)

    # Registration ping + keepalive thread
    threading.Thread(target=register_loop, args=(sock, dest, args.timeout),
                     daemon=True).start()

    print(f"Listening for {args.width}x{args.height} YUV420 frames from {args.host}:{args.port}")
    print("Press 'q' in the display window to quit.\n")

    # chunk buffer: frame_seq -> {chunk_seq: bytes}
    frames: dict = defaultdict(dict)
    last_complete = -1

    while True:
        try:
            pkt, _ = sock.recvfrom(65535)
        except socket.timeout:
            continue

        if len(pkt) < HEADER_SIZE:
            continue

        frame_seq, chunk_seq, total_chunks = HEADER.unpack_from(pkt)
        payload = pkt[HEADER_SIZE:]

        frames[frame_seq][chunk_seq] = payload

        # Assemble when all chunks have arrived
        if len(frames[frame_seq]) == total_chunks and frame_seq > last_complete:
            last_complete = frame_seq
            raw = b"".join(frames[frame_seq][i] for i in range(total_chunks))

            # Discard old partial frames
            for old in [k for k in frames if k <= frame_seq - 10]:
                del frames[old]

            if len(raw) < expected_bytes:
                print(f"Frame {frame_seq}: short ({len(raw)} < {expected_bytes}) — skipping")
                continue

            yuv = np.frombuffer(raw[:expected_bytes], dtype=np.uint8)
            yuv = yuv.reshape((args.height * 3 // 2, args.width))
            bgr = cv2.cvtColor(yuv, cv2.COLOR_YUV2BGR_I420)

            if args.save:
                cv2.imwrite(args.save, bgr)
                print(f"Frame {frame_seq}: saved {args.width}x{args.height} to {args.save}")
                break

            cv2.imshow("picam-raw", bgr)
            if cv2.waitKey(1) & 0xFF == ord("q"):
                break

    if not args.save:
        cv2.destroyAllWindows()
    sock.close()


if __name__ == "__main__":
    main()
