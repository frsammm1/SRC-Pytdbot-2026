"""Standalone tests for large-file split helpers (no bot deps)."""
import os
import tempfile
import unittest

SPLIT_THRESHOLD = 2030 * 1024 * 1024
SPLIT_COPY_CHUNK = 8 * 1024 * 1024
TWO_GIB = 2 * 1024 * 1024 * 1024


def stream_copy_range(src_path: str, dest_path: str, offset: int, length: int) -> int:
    os.makedirs(os.path.dirname(dest_path) or "/tmp", exist_ok=True)
    written = 0
    with open(src_path, "rb") as src, open(dest_path, "wb") as dst:
        src.seek(offset)
        remaining = length
        while remaining > 0:
            chunk = src.read(min(SPLIT_COPY_CHUNK, remaining))
            if not chunk:
                break
            dst.write(chunk)
            written += len(chunk)
            remaining -= len(chunk)
    return written


def part_caption(base: str, part_num: int) -> str:
    base = (base or "").rstrip()
    if base:
        return f"{base}\n\npart {part_num}"
    return f"part {part_num}"


class StreamSplitTests(unittest.TestCase):
    def test_stream_copy_range_splits_without_loading_all(self):
        data = os.urandom(64 * 1024)
        with tempfile.TemporaryDirectory() as td:
            src = os.path.join(td, "src.bin")
            with open(src, "wb") as f:
                f.write(data)
            p1 = os.path.join(td, "part1.bin")
            p2 = os.path.join(td, "part2.bin")
            n1 = stream_copy_range(src, p1, 0, 40 * 1024)
            n2 = stream_copy_range(src, p2, 40 * 1024, 24 * 1024)
            self.assertEqual(n1, 40 * 1024)
            self.assertEqual(n2, 24 * 1024)
            with open(p1, "rb") as f:
                self.assertEqual(f.read(), data[:40 * 1024])
            with open(p2, "rb") as f:
                self.assertEqual(f.read(), data[40 * 1024:])

    def test_split_threshold_is_under_2gib(self):
        self.assertLess(SPLIT_THRESHOLD, TWO_GIB)
        self.assertGreater(SPLIT_THRESHOLD, int(1.9 * 1024 * 1024 * 1024))

    def test_part_caption_appends_part_n(self):
        self.assertEqual(part_caption("Hello world", 1), "Hello world\n\npart 1")
        self.assertEqual(part_caption("Hello world", 2), "Hello world\n\npart 2")
        self.assertEqual(part_caption("", 1), "part 1")

    def test_391gb_video_becomes_two_parts_under_2gib(self):
        for size in (int(3.91 * 1024 ** 3), int(3.91 * 1000 ** 3)):
            parts = (size + SPLIT_THRESHOLD - 1) // SPLIT_THRESHOLD
            self.assertEqual(parts, 2, f"size={size} parts={parts}")
            p1 = min(SPLIT_THRESHOLD, size)
            p2 = size - p1
            self.assertLess(p1, TWO_GIB)
            self.assertLess(p2, TWO_GIB)
            self.assertGreater(p1, int(1.9 * 1024 * 1024 * 1024))


if __name__ == "__main__":
    unittest.main()
