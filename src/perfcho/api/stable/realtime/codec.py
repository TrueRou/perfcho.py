"""Bounded binary reader and low-copy writer for osu! Stable Bancho packets."""

from __future__ import annotations

import struct
from collections.abc import Collection, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import IntEnum
from types import TracebackType
from typing import Self

from .models import (
    MATCH_SLOT_COUNT,
    OCCUPIED_SLOT_MASK,
    Channel,
    ClientPacket,
    ClientStatus,
    Message,
    MultiplayerMatch,
    ReplayAction,
    ReplayFrame,
    ReplayFrameBundle,
    ScoreFrame,
    ServerPacket,
    UserPresence,
    UserStats,
)

HEADER_STRUCT = struct.Struct("<HxI")
HEADER_SIZE = HEADER_STRUCT.size
SCORE_FRAME_STRUCT = struct.Struct("<iBHHHHHHiHHBBBB")
REPLAY_FRAME_STRUCT = struct.Struct("<BBffi")

_I8 = struct.Struct("<b")
_U8 = struct.Struct("<B")
_I16 = struct.Struct("<h")
_U16 = struct.Struct("<H")
_I32 = struct.Struct("<i")
_U32 = struct.Struct("<I")
_I64 = struct.Struct("<q")
_U64 = struct.Struct("<Q")
_F16 = struct.Struct("<e")
_F32 = struct.Struct("<f")
_F64 = struct.Struct("<d")
_BOOL = struct.Struct("<?")

PacketEnum = ClientPacket | ServerPacket
PacketEnumType = type[ClientPacket] | type[ServerPacket]
ReadableBuffer = bytes | bytearray | memoryview


class ProtocolError(ValueError):
    """Base class for controlled Stable wire-protocol failures."""


class ProtocolStateError(ProtocolError):
    """Raised when a reader or writer operation is invalid in its current state."""


class LimitExceededError(ProtocolError):
    """Base class for configured resource-bound violations."""


class BodyTooLargeError(LimitExceededError):
    """Raised when a complete Bancho body exceeds its configured bound."""


class PacketTooLargeError(LimitExceededError):
    """Raised when a packet payload exceeds its configured bound."""


class PacketCountExceededError(LimitExceededError):
    """Raised when a body contains more packets than permitted."""


class StringTooLargeError(LimitExceededError):
    """Raised when a string's encoded payload exceeds its configured bound."""


class ListTooLargeError(LimitExceededError):
    """Raised when a variable-length list exceeds its configured item bound."""


class FrameCountExceededError(LimitExceededError):
    """Raised when a spectator bundle exceeds its configured frame bound."""


class TruncatedDataError(ProtocolError):
    """Base class for an incomplete Stable wire structure."""


class TruncatedHeaderError(TruncatedDataError):
    """Raised when bytes remain but cannot form a complete packet header."""


class TruncatedPayloadError(TruncatedDataError):
    """Raised when a packet or field declares more bytes than remain."""


class InvalidStringMarkerError(ProtocolError):
    """Raised when a Stable string does not begin with 0x00 or 0x0b."""


class InvalidStringEncodingError(ProtocolError):
    """Raised when a Stable string contains invalid UTF-8."""


class MalformedULEB128Error(ProtocolError):
    """Raised for truncated, overflowing, or non-canonical ULEB128 values."""


class InvalidStructureError(ProtocolError):
    """Raised when a compound Stable structure is internally inconsistent."""


class TrailingDataError(ProtocolError):
    """Raised when a caller requires a payload to have been consumed exactly."""


@dataclass(frozen=True, slots=True)
class CodecLimits:
    """Resource bounds applied while encoding and decoding Stable traffic."""

    max_body_size: int = 16 * 1024 * 1024
    max_packet_size: int = 2 * 1024 * 1024
    max_packet_count: int = 4096
    max_string_size: int = 64 * 1024
    max_list_length: int = 8192
    max_frame_count: int = 4096
    max_uleb128_bytes: int = 5

    def __post_init__(self) -> None:
        """Reject invalid limits before they are used for untrusted traffic."""
        for name in (
            "max_body_size",
            "max_packet_size",
            "max_packet_count",
            "max_string_size",
            "max_list_length",
            "max_frame_count",
        ):
            if getattr(self, name) < 0:
                msg = f"{name} must be non-negative"
                raise ValueError(msg)
        if not 1 <= self.max_uleb128_bytes <= 5:
            msg = "max_uleb128_bytes must be between 1 and 5"
            raise ValueError(msg)


DEFAULT_LIMITS = CodecLimits()


@dataclass(frozen=True, slots=True)
class Packet:
    """A framed packet with an exact, independently bounded payload reader."""

    packet_id: int
    packet_type: PacketEnum | None
    payload: PacketReader

    @property
    def known(self) -> bool:
        """Return whether the identifier belongs to the configured inventory."""
        return self.packet_type is not None

    @property
    def payload_view(self) -> memoryview:
        """Return the complete exact payload slice without copying it."""
        return self.payload.view


class PacketReader(Iterator[Packet]):
    """Read framed packets or fields from a bounded memoryview."""

    __slots__ = ("_packet_count", "_packet_enum", "_position", "_view", "limits")

    def __init__(
        self,
        data: ReadableBuffer,
        *,
        packet_enum: PacketEnumType = ClientPacket,
        limits: CodecLimits = DEFAULT_LIMITS,
        _payload: bool = False,
    ) -> None:
        """Create a reader without copying a contiguous byte-oriented buffer."""
        view = data if isinstance(data, memoryview) else memoryview(data)
        try:
            view = view.cast("B")
        except (TypeError, ValueError) as exc:
            msg = "Stable packet input must be a contiguous byte buffer"
            raise TypeError(msg) from exc
        view = view.toreadonly()

        if not _payload and view.nbytes > limits.max_body_size:
            raise BodyTooLargeError(f"body length {view.nbytes} exceeds limit {limits.max_body_size}")
        if _payload and view.nbytes > limits.max_packet_size:
            raise PacketTooLargeError(f"packet length {view.nbytes} exceeds limit {limits.max_packet_size}")

        self._view = view
        self._position = 0
        self._packet_count = 0
        self._packet_enum = packet_enum
        self.limits = limits

    @property
    def view(self) -> memoryview:
        """Return the complete backing view, including bytes already consumed."""
        return self._view

    @property
    def position(self) -> int:
        """Return the current byte offset in this reader."""
        return self._position

    @property
    def remaining(self) -> int:
        """Return the number of unread bytes."""
        return self._view.nbytes - self._position

    def __iter__(self) -> Self:
        """Return this packet stream iterator."""
        return self

    def __next__(self) -> Packet:
        """Read the next framed packet from the body."""
        if self.remaining == 0:
            raise StopIteration
        return self.read_packet()

    def read_packet(self) -> Packet:
        """Read one header and expose exactly its declared payload slice."""
        if self.remaining == 0:
            raise StopIteration
        if self.remaining < HEADER_SIZE:
            raise TruncatedHeaderError(
                f"packet header requires {HEADER_SIZE} bytes, only {self.remaining} remain at offset {self._position}",
            )
        if self._packet_count >= self.limits.max_packet_count:
            raise PacketCountExceededError(f"packet count exceeds limit {self.limits.max_packet_count}")

        packet_id, payload_size = HEADER_STRUCT.unpack_from(self._view, self._position)
        if payload_size > self.limits.max_packet_size:
            raise PacketTooLargeError(f"packet length {payload_size} exceeds limit {self.limits.max_packet_size}")

        payload_start = self._position + HEADER_SIZE
        payload_end = payload_start + payload_size
        if payload_end > self._view.nbytes:
            available = self._view.nbytes - payload_start
            raise TruncatedPayloadError(f"packet {packet_id} declares {payload_size} bytes, only {available} remain")

        # Advancing the parent before handing out the child prevents partial
        # field reads from changing where the next packet begins.
        payload_view = self._view[payload_start:payload_end]
        self._position = payload_end
        self._packet_count += 1
        try:
            packet_type: PacketEnum | None = self._packet_enum(packet_id)
        except ValueError:
            packet_type = None

        return Packet(
            packet_id=packet_id,
            packet_type=packet_type,
            payload=PacketReader(
                payload_view,
                packet_enum=self._packet_enum,
                limits=self.limits,
                _payload=True,
            ),
        )

    def _take(self, size: int) -> memoryview:
        if size < 0:
            msg = "read size must be non-negative"
            raise ValueError(msg)
        end = self._position + size
        if end > self._view.nbytes:
            raise TruncatedPayloadError(
                f"field requires {size} bytes, only {self.remaining} remain at offset {self._position}",
            )
        value = self._view[self._position : end]
        self._position = end
        return value

    def _unpack(self, value_struct: struct.Struct) -> int | float | bool:
        value = value_struct.unpack_from(self._take(value_struct.size))[0]
        return value

    def read_bytes(self, size: int) -> memoryview:
        """Read an exact zero-copy byte slice."""
        return self._take(size)

    def read_remaining(self) -> memoryview:
        """Consume and return all remaining bytes without copying them."""
        return self._take(self.remaining)

    def read_raw(self, size: int | None = None) -> memoryview:
        """Read a fixed number of bytes, or all remaining bytes when omitted."""
        return self.read_remaining() if size is None else self.read_bytes(size)

    def require_exhausted(self) -> None:
        """Require that the current bounded view has been consumed exactly."""
        if self.remaining:
            raise TrailingDataError(f"{self.remaining} trailing payload bytes remain")

    def read_i8(self) -> int:
        """Read a little-endian signed 8-bit integer."""
        return int(self._unpack(_I8))

    def read_u8(self) -> int:
        """Read a little-endian unsigned 8-bit integer."""
        return int(self._unpack(_U8))

    def read_i16(self) -> int:
        """Read a little-endian signed 16-bit integer."""
        return int(self._unpack(_I16))

    def read_u16(self) -> int:
        """Read a little-endian unsigned 16-bit integer."""
        return int(self._unpack(_U16))

    def read_i32(self) -> int:
        """Read a little-endian signed 32-bit integer."""
        return int(self._unpack(_I32))

    def read_u32(self) -> int:
        """Read a little-endian unsigned 32-bit integer."""
        return int(self._unpack(_U32))

    def read_i64(self) -> int:
        """Read a little-endian signed 64-bit integer."""
        return int(self._unpack(_I64))

    def read_u64(self) -> int:
        """Read a little-endian unsigned 64-bit integer."""
        return int(self._unpack(_U64))

    def read_f16(self) -> float:
        """Read a little-endian IEEE 754 half-precision float."""
        return float(self._unpack(_F16))

    def read_f32(self) -> float:
        """Read a little-endian IEEE 754 single-precision float."""
        return float(self._unpack(_F32))

    def read_f64(self) -> float:
        """Read a little-endian IEEE 754 double-precision float."""
        return float(self._unpack(_F64))

    def read_bool(self) -> bool:
        """Read a one-byte Stable boolean."""
        value = self.read_u8()
        if value not in (0, 1):
            raise InvalidStructureError(f"invalid Stable boolean value {value}")
        return bool(value)

    def read_uleb128(self) -> int:
        """Read a canonical unsigned 32-bit LEB128 value within configured bounds."""
        value = 0
        for index in range(self.limits.max_uleb128_bytes):
            if self.remaining == 0:
                raise MalformedULEB128Error("truncated ULEB128 value")
            byte = self.read_u8()
            payload = byte & 0x7F
            if index == 4 and payload > 0x0F:
                raise MalformedULEB128Error("ULEB128 value overflows 32 bits")
            value |= payload << (index * 7)
            if byte & 0x80 == 0:
                if index > 0 and payload == 0:
                    raise MalformedULEB128Error("non-canonical ULEB128 value")
                return value
        raise MalformedULEB128Error(
            f"ULEB128 value exceeds {self.limits.max_uleb128_bytes} bytes",
        )

    def read_string(self) -> str:
        """Read a nullable-marker Stable UTF-8 string with bounded ULEB128 length."""
        marker = self.read_u8()
        if marker == 0x00:
            return ""
        if marker != 0x0B:
            raise InvalidStringMarkerError(f"invalid Stable string marker 0x{marker:02x}")

        length = self.read_uleb128()
        if length > self.limits.max_string_size:
            raise StringTooLargeError(f"string length {length} exceeds limit {self.limits.max_string_size}")
        encoded = self._take(length)
        try:
            return encoded.tobytes().decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidStringEncodingError("Stable string is not valid UTF-8") from exc

    def _read_i32_list(
        self,
        length_struct: struct.Struct,
        *,
        max_length: int | None = None,
    ) -> tuple[int, ...]:
        count = int(self._unpack(length_struct))
        effective_limit = self.limits.max_list_length
        if max_length is not None:
            if isinstance(max_length, bool) or not isinstance(max_length, int) or max_length < 0:
                raise ValueError("max_length must be a non-negative integer")
            effective_limit = min(effective_limit, max_length)
        if count > effective_limit:
            raise ListTooLargeError(f"list length {count} exceeds limit {effective_limit}")
        if count == 0:
            return ()
        encoded = self._take(count * _I32.size)
        return struct.unpack_from(f"<{count}i", encoded)

    def read_i32_list_u16(self, *, max_length: int | None = None) -> tuple[int, ...]:
        """Read signed i32 values prefixed by an unsigned 16-bit item count."""
        return self._read_i32_list(_U16, max_length=max_length)

    def read_i32_list_u32(self, *, max_length: int | None = None) -> tuple[int, ...]:
        """Read signed i32 values prefixed by an unsigned 32-bit item count."""
        return self._read_i32_list(_U32, max_length=max_length)

    def read_message(self) -> Message:
        """Read a Stable chat message structure."""
        return Message(
            sender=self.read_string(),
            text=self.read_string(),
            recipient=self.read_string(),
            sender_id=self.read_i32(),
        )

    def read_channel(self) -> Channel:
        """Read a Stable channel description structure."""
        return Channel(
            name=self.read_string(),
            topic=self.read_string(),
            player_count=self.read_u16(),
        )

    def read_client_status(self) -> ClientStatus:
        """Read a Stable client change-action status structure."""
        return ClientStatus(
            action=self.read_u8(),
            info_text=self.read_string(),
            beatmap_md5=self.read_string(),
            mods=self.read_u32(),
            mode=self.read_u8(),
            beatmap_id=self.read_i32(),
        )

    def read_user_presence(self) -> UserPresence:
        """Read a Stable server user-presence structure."""
        user_id = self.read_i32()
        username = self.read_string()
        utc_offset = self.read_u8() - 24
        country_code = self.read_u8()
        privileges_and_mode = self.read_u8()
        return UserPresence(
            user_id=user_id,
            username=username,
            utc_offset=utc_offset,
            country_code=country_code,
            privileges=privileges_and_mode & 0x1F,
            mode=privileges_and_mode >> 5,
            longitude=self.read_f32(),
            latitude=self.read_f32(),
            global_rank=self.read_i32(),
        )

    def read_user_stats(self) -> UserStats:
        """Read a Stable server user-statistics structure."""
        return UserStats(
            user_id=self.read_i32(),
            action=self.read_u8(),
            info_text=self.read_string(),
            beatmap_md5=self.read_string(),
            mods=self.read_i32(),
            mode=self.read_u8(),
            beatmap_id=self.read_i32(),
            ranked_score=self.read_i64(),
            accuracy=self.read_f32(),
            play_count=self.read_i32(),
            total_score=self.read_i64(),
            global_rank=self.read_i32(),
            performance=self.read_u16(),
        )

    def read_multiplayer_match(self) -> MultiplayerMatch:
        """Read a complete Stable multiplayer match structure."""
        match_id = self.read_i16()
        in_progress = self.read_bool()
        match_type = self.read_i8()
        mods = self.read_i32()
        name = self.read_string()
        password = self.read_string()
        beatmap_name = self.read_string()
        beatmap_id = self.read_i32()
        beatmap_md5 = self.read_string()
        slot_statuses = tuple(self.read_u8() for _ in range(MATCH_SLOT_COUNT))
        slot_teams = tuple(self.read_u8() for _ in range(MATCH_SLOT_COUNT))
        slot_user_ids = tuple(self.read_i32() if status & OCCUPIED_SLOT_MASK else None for status in slot_statuses)
        host_id = self.read_i32()
        mode = self.read_u8()
        win_condition = self.read_u8()
        team_type = self.read_u8()
        freemods = self.read_bool()
        slot_mods = tuple(self.read_i32() for _ in range(MATCH_SLOT_COUNT)) if freemods else ()
        seed = self.read_i32()
        return MultiplayerMatch(
            match_id=match_id,
            in_progress=in_progress,
            match_type=match_type,
            mods=mods,
            name=name,
            password=password,
            beatmap_name=beatmap_name,
            beatmap_id=beatmap_id,
            beatmap_md5=beatmap_md5,
            slot_statuses=slot_statuses,
            slot_teams=slot_teams,
            slot_user_ids=slot_user_ids,
            host_id=host_id,
            mode=mode,
            win_condition=win_condition,
            team_type=team_type,
            freemods=freemods,
            slot_mods=slot_mods,
            seed=seed,
        )

    def read_score_frame(self) -> ScoreFrame:
        """Read a Stable score frame, including optional ScoreV2 portions."""
        values = SCORE_FRAME_STRUCT.unpack_from(self._take(SCORE_FRAME_STRUCT.size))
        if values[11] not in (0, 1):
            raise InvalidStructureError(f"invalid score-frame perfect boolean {values[11]}")
        if values[14] not in (0, 1):
            raise InvalidStructureError(f"invalid score-frame ScoreV2 boolean {values[14]}")
        score_v2 = bool(values[14])
        return ScoreFrame(
            time=values[0],
            frame_id=values[1],
            count_300=values[2],
            count_100=values[3],
            count_50=values[4],
            count_geki=values[5],
            count_katu=values[6],
            count_miss=values[7],
            total_score=values[8],
            max_combo=values[9],
            current_combo=values[10],
            perfect=bool(values[11]),
            current_hp=values[12],
            tag_byte=values[13],
            score_v2=score_v2,
            combo_portion=self.read_f64() if score_v2 else None,
            bonus_portion=self.read_f64() if score_v2 else None,
        )

    def read_replay_frame(self) -> ReplayFrame:
        """Read one Stable spectator input frame."""
        values = REPLAY_FRAME_STRUCT.unpack_from(self._take(REPLAY_FRAME_STRUCT.size))
        return ReplayFrame(
            button_state=values[0],
            taiko_byte=values[1],
            x=values[2],
            y=values[3],
            time=values[4],
        )

    def read_replay_frame_bundle(self) -> ReplayFrameBundle:
        """Read and bound a Stable spectator replay-frame bundle."""
        extra = self.read_i32()
        frame_count = self.read_u16()
        if frame_count > self.limits.max_frame_count:
            raise FrameCountExceededError(
                f"frame count {frame_count} exceeds limit {self.limits.max_frame_count}",
            )
        frames = tuple(self.read_replay_frame() for _ in range(frame_count))
        action_value = self.read_u8()
        try:
            action = ReplayAction(action_value)
        except ValueError as error:
            raise InvalidStructureError(f"invalid replay action {action_value}") from error
        score_frame = self.read_score_frame()
        sequence = self.read_u16()
        self.require_exhausted()
        return ReplayFrameBundle(
            frames=frames,
            score_frame=score_frame,
            action=action,
            extra=extra,
            sequence=sequence,
        )


class _PacketContext(AbstractContextManager["PacketWriter"]):
    __slots__ = ("_writer",)

    def __init__(self, writer: PacketWriter) -> None:
        self._writer = writer

    def __enter__(self) -> PacketWriter:
        return self._writer

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if exc_type is None:
            self._writer.end_packet()
        else:
            self._writer.cancel_packet()
        return None


class PacketWriter:
    """Build one or more Stable packets in one append-only bytearray."""

    __slots__ = ("_buffer", "_packet_count", "_packet_start", "limits")

    def __init__(self, *, limits: CodecLimits = DEFAULT_LIMITS) -> None:
        """Create an empty bounded writer."""
        self._buffer = bytearray()
        self._packet_count = 0
        self._packet_start: int | None = None
        self.limits = limits

    def __len__(self) -> int:
        """Return the current encoded byte length."""
        return len(self._buffer)

    @property
    def packet_open(self) -> bool:
        """Return whether a packet body is currently being written."""
        return self._packet_start is not None

    def _ensure_capacity(self, additional: int) -> None:
        target_size = len(self._buffer) + additional
        if target_size > self.limits.max_body_size:
            raise BodyTooLargeError(f"encoded body length {target_size} exceeds limit {self.limits.max_body_size}")
        if self._packet_start is not None:
            payload_size = target_size - self._packet_start - HEADER_SIZE
            if payload_size > self.limits.max_packet_size:
                raise PacketTooLargeError(
                    f"encoded packet length {payload_size} exceeds limit {self.limits.max_packet_size}",
                )

    def _reserve(self, size: int) -> int:
        self._ensure_capacity(size)
        offset = len(self._buffer)
        self._buffer.extend(bytes(size))
        return offset

    def _pack(self, value_struct: struct.Struct, value: object) -> None:
        offset = self._reserve(value_struct.size)
        try:
            value_struct.pack_into(self._buffer, offset, value)
        except (OverflowError, struct.error) as exc:
            del self._buffer[offset:]
            raise InvalidStructureError(f"value {value!r} does not fit its Stable wire type") from exc

    def begin_packet(self, packet_id: int | IntEnum) -> None:
        """Reserve a header and begin an append-only packet body."""
        if self._packet_start is not None:
            raise ProtocolStateError("cannot begin a packet while another packet is open")
        if self._packet_count >= self.limits.max_packet_count:
            raise PacketCountExceededError(f"packet count exceeds limit {self.limits.max_packet_count}")
        numeric_id = int(packet_id)
        if not 0 <= numeric_id <= 0xFFFF:
            raise InvalidStructureError(f"packet identifier {numeric_id} is outside u16 range")
        self._ensure_capacity(HEADER_SIZE)
        self._packet_start = len(self._buffer)
        self._buffer.extend(bytes(HEADER_SIZE))
        _U16.pack_into(self._buffer, self._packet_start, numeric_id)

    def end_packet(self) -> None:
        """Finalize the open packet by filling its reserved body length."""
        if self._packet_start is None:
            raise ProtocolStateError("no packet is open")
        packet_start = self._packet_start
        payload_size = len(self._buffer) - packet_start - HEADER_SIZE
        _U32.pack_into(self._buffer, packet_start + 3, payload_size)
        self._packet_start = None
        self._packet_count += 1

    def cancel_packet(self) -> None:
        """Discard the open packet and all bytes written to its body."""
        if self._packet_start is None:
            raise ProtocolStateError("no packet is open")
        del self._buffer[self._packet_start :]
        self._packet_start = None

    def packet(self, packet_id: int | IntEnum) -> AbstractContextManager[PacketWriter]:
        """Return a context manager that finalizes or rolls back one packet."""
        self.begin_packet(packet_id)
        return _PacketContext(self)

    def write_packet(self, packet_id: int | IntEnum, payload: ReadableBuffer = b"") -> None:
        """Append a complete packet containing an already encoded payload."""
        with self.packet(packet_id):
            self.write_raw(payload)

    def to_bytes(self) -> bytes:
        """Return the completed packet stream as immutable bytes."""
        if self._packet_start is not None:
            raise ProtocolStateError("cannot export while a packet is open")
        return bytes(self._buffer)

    def write_raw(self, value: ReadableBuffer) -> None:
        """Append raw bytes to the current output."""
        view = value if isinstance(value, memoryview) else memoryview(value)
        try:
            view = view.cast("B")
        except (TypeError, ValueError) as exc:
            msg = "raw Stable output must be a contiguous byte buffer"
            raise TypeError(msg) from exc
        self._ensure_capacity(view.nbytes)
        self._buffer.extend(view)

    def write_i8(self, value: int) -> None:
        """Write a little-endian signed 8-bit integer."""
        self._pack(_I8, value)

    def write_u8(self, value: int) -> None:
        """Write a little-endian unsigned 8-bit integer."""
        self._pack(_U8, value)

    def write_i16(self, value: int) -> None:
        """Write a little-endian signed 16-bit integer."""
        self._pack(_I16, value)

    def write_u16(self, value: int) -> None:
        """Write a little-endian unsigned 16-bit integer."""
        self._pack(_U16, value)

    def write_i32(self, value: int) -> None:
        """Write a little-endian signed 32-bit integer."""
        self._pack(_I32, value)

    def write_u32(self, value: int) -> None:
        """Write a little-endian unsigned 32-bit integer."""
        self._pack(_U32, value)

    def write_i64(self, value: int) -> None:
        """Write a little-endian signed 64-bit integer."""
        self._pack(_I64, value)

    def write_u64(self, value: int) -> None:
        """Write a little-endian unsigned 64-bit integer."""
        self._pack(_U64, value)

    def write_f16(self, value: float) -> None:
        """Write a little-endian IEEE 754 half-precision float."""
        self._pack(_F16, value)

    def write_f32(self, value: float) -> None:
        """Write a little-endian IEEE 754 single-precision float."""
        self._pack(_F32, value)

    def write_f64(self, value: float) -> None:
        """Write a little-endian IEEE 754 double-precision float."""
        self._pack(_F64, value)

    def write_bool(self, value: bool) -> None:
        """Write a one-byte Stable boolean."""
        if not isinstance(value, bool):
            raise InvalidStructureError(f"Stable boolean must be bool, received {value!r}")
        self._pack(_BOOL, value)

    def write_uleb128(self, value: int) -> None:
        """Write a canonical unsigned 32-bit LEB128 value."""
        if not 0 <= value <= 0xFFFFFFFF:
            raise InvalidStructureError(f"ULEB128 value {value} is outside u32 range")
        byte_count = max(1, (value.bit_length() + 6) // 7)
        if byte_count > self.limits.max_uleb128_bytes:
            raise MalformedULEB128Error(
                f"ULEB128 value requires more than {self.limits.max_uleb128_bytes} bytes",
            )
        remaining = value
        while True:
            byte = remaining & 0x7F
            remaining >>= 7
            if remaining:
                byte |= 0x80
            self.write_u8(byte)
            if not remaining:
                break

    def write_string(self, value: str) -> None:
        """Write a bounded UTF-8 Stable string with marker and ULEB128 length."""
        if not value:
            self.write_u8(0x00)
            return
        try:
            encoded = value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise InvalidStringEncodingError("Stable string is not valid Unicode") from exc
        if len(encoded) > self.limits.max_string_size:
            raise StringTooLargeError(
                f"string length {len(encoded)} exceeds limit {self.limits.max_string_size}",
            )
        self.write_u8(0x0B)
        self.write_uleb128(len(encoded))
        self.write_raw(encoded)

    def _write_i32_list(self, values: Collection[int], length_struct: struct.Struct) -> None:
        count = len(values)
        if count > self.limits.max_list_length:
            raise ListTooLargeError(f"list length {count} exceeds limit {self.limits.max_list_length}")
        if count > (1 << (length_struct.size * 8)) - 1:
            raise ListTooLargeError(f"list length {count} cannot fit its wire prefix")
        start = len(self._buffer)
        self._pack(length_struct, count)
        offset = self._reserve(count * _I32.size)
        try:
            for index, value in enumerate(values):
                _I32.pack_into(self._buffer, offset + index * _I32.size, value)
        except (OverflowError, struct.error) as exc:
            del self._buffer[start:]
            raise InvalidStructureError("list item does not fit the Stable i32 wire type") from exc

    def write_i32_list_u16(self, values: Collection[int]) -> None:
        """Write signed i32 values with an unsigned 16-bit item count."""
        self._write_i32_list(values, _U16)

    def write_i32_list_u32(self, values: Collection[int]) -> None:
        """Write signed i32 values with an unsigned 32-bit item count."""
        self._write_i32_list(values, _U32)

    def write_message(self, message: Message) -> None:
        """Write a Stable chat message structure."""
        self.write_string(message.sender)
        self.write_string(message.text)
        self.write_string(message.recipient)
        self.write_i32(message.sender_id)

    def write_channel(self, channel: Channel) -> None:
        """Write a Stable channel description structure."""
        self.write_string(channel.name)
        self.write_string(channel.topic)
        self.write_u16(channel.player_count)

    def write_client_status(self, status: ClientStatus) -> None:
        """Write a Stable client change-action status structure."""
        self.write_u8(status.action)
        self.write_string(status.info_text)
        self.write_string(status.beatmap_md5)
        self.write_u32(status.mods)
        self.write_u8(status.mode)
        self.write_i32(status.beatmap_id)

    def write_user_presence(self, presence: UserPresence) -> None:
        """Write a Stable server user-presence structure."""
        if not 0 <= presence.mode <= 7:
            raise InvalidStructureError(f"mode {presence.mode} is outside packed 3-bit range")
        if not 0 <= presence.privileges <= 0x1F:
            raise InvalidStructureError(f"privileges {presence.privileges} are outside packed 5-bit range")
        self.write_i32(presence.user_id)
        self.write_string(presence.username)
        self.write_u8(presence.utc_offset + 24)
        self.write_u8(presence.country_code)
        self.write_u8(presence.privileges | (presence.mode << 5))
        self.write_f32(presence.longitude)
        self.write_f32(presence.latitude)
        self.write_i32(presence.global_rank)

    def write_user_stats(self, stats: UserStats) -> None:
        """Write a Stable server user-statistics structure."""
        self.write_i32(stats.user_id)
        self.write_u8(stats.action)
        self.write_string(stats.info_text)
        self.write_string(stats.beatmap_md5)
        self.write_i32(stats.mods)
        self.write_u8(stats.mode)
        self.write_i32(stats.beatmap_id)
        self.write_i64(stats.ranked_score)
        self.write_f32(stats.accuracy)
        self.write_i32(stats.play_count)
        self.write_i64(stats.total_score)
        self.write_i32(stats.global_rank)
        self.write_u16(stats.performance)

    @staticmethod
    def _require_slots(name: str, values: tuple[object, ...]) -> None:
        if len(values) != MATCH_SLOT_COUNT:
            raise InvalidStructureError(f"{name} must contain exactly {MATCH_SLOT_COUNT} entries")

    def write_multiplayer_match(self, match: MultiplayerMatch, *, send_password: bool = True) -> None:
        """Write a complete Stable multiplayer match structure."""
        self._require_slots("slot_statuses", match.slot_statuses)
        self._require_slots("slot_teams", match.slot_teams)
        self._require_slots("slot_user_ids", match.slot_user_ids)
        if match.freemods:
            self._require_slots("slot_mods", match.slot_mods)
        elif match.slot_mods:
            raise InvalidStructureError("slot_mods must be empty when freemods is disabled")
        for status, user_id in zip(match.slot_statuses, match.slot_user_ids, strict=True):
            if bool(status & OCCUPIED_SLOT_MASK) != (user_id is not None):
                raise InvalidStructureError("slot status and user identifier occupancy disagree")

        self.write_i16(match.match_id)
        self.write_bool(match.in_progress)
        self.write_i8(match.match_type)
        self.write_i32(match.mods)
        self.write_string(match.name)
        if match.password and not send_password:
            self.write_raw(b"\x0b\x00")
        else:
            self.write_string(match.password)
        self.write_string(match.beatmap_name)
        self.write_i32(match.beatmap_id)
        self.write_string(match.beatmap_md5)
        for status in match.slot_statuses:
            self.write_u8(status)
        for team in match.slot_teams:
            self.write_u8(team)
        for user_id in match.slot_user_ids:
            if user_id is not None:
                self.write_i32(user_id)
        self.write_i32(match.host_id)
        self.write_u8(match.mode)
        self.write_u8(match.win_condition)
        self.write_u8(match.team_type)
        self.write_bool(match.freemods)
        for mods in match.slot_mods:
            self.write_i32(mods)
        self.write_i32(match.seed)

    def write_score_frame(self, frame: ScoreFrame) -> None:
        """Write a Stable score frame, including optional ScoreV2 portions."""
        if not isinstance(frame.perfect, bool) or not isinstance(frame.score_v2, bool):
            raise InvalidStructureError("score-frame boolean fields must be bool")
        if frame.score_v2 and (frame.combo_portion is None or frame.bonus_portion is None):
            raise InvalidStructureError("ScoreV2 frames require combo and bonus portions")
        if not frame.score_v2 and (frame.combo_portion is not None or frame.bonus_portion is not None):
            raise InvalidStructureError("non-ScoreV2 frames cannot contain ScoreV2 portions")
        offset = self._reserve(SCORE_FRAME_STRUCT.size)
        try:
            SCORE_FRAME_STRUCT.pack_into(
                self._buffer,
                offset,
                frame.time,
                frame.frame_id,
                frame.count_300,
                frame.count_100,
                frame.count_50,
                frame.count_geki,
                frame.count_katu,
                frame.count_miss,
                frame.total_score,
                frame.max_combo,
                frame.current_combo,
                frame.perfect,
                frame.current_hp,
                frame.tag_byte,
                frame.score_v2,
            )
        except (OverflowError, struct.error) as exc:
            del self._buffer[offset:]
            raise InvalidStructureError("score frame value does not fit its Stable wire type") from exc
        if frame.score_v2:
            assert frame.combo_portion is not None
            assert frame.bonus_portion is not None
            self.write_f64(frame.combo_portion)
            self.write_f64(frame.bonus_portion)

    def write_replay_frame(self, frame: ReplayFrame) -> None:
        """Write one Stable spectator input frame."""
        offset = self._reserve(REPLAY_FRAME_STRUCT.size)
        try:
            REPLAY_FRAME_STRUCT.pack_into(
                self._buffer,
                offset,
                frame.button_state,
                frame.taiko_byte,
                frame.x,
                frame.y,
                frame.time,
            )
        except (OverflowError, struct.error) as exc:
            del self._buffer[offset:]
            raise InvalidStructureError("replay frame value does not fit its Stable wire type") from exc

    def write_replay_frame_bundle(self, bundle: ReplayFrameBundle) -> None:
        """Write a bounded Stable spectator replay-frame bundle."""
        frame_count = len(bundle.frames)
        if frame_count > self.limits.max_frame_count:
            raise FrameCountExceededError(
                f"frame count {frame_count} exceeds limit {self.limits.max_frame_count}",
            )
        if frame_count > 0xFFFF:
            raise FrameCountExceededError(f"frame count {frame_count} cannot fit its wire prefix")
        self.write_i32(bundle.extra)
        self.write_u16(frame_count)
        for frame in bundle.frames:
            self.write_replay_frame(frame)
        self.write_u8(bundle.action)
        self.write_score_frame(bundle.score_frame)
        self.write_u16(bundle.sequence)


def build_packet(
    packet_id: int | IntEnum,
    payload: ReadableBuffer = b"",
    *,
    limits: CodecLimits = DEFAULT_LIMITS,
) -> bytes:
    """Build one bounded Stable packet from an already encoded payload."""
    writer = PacketWriter(limits=limits)
    writer.write_packet(packet_id, payload)
    return writer.to_bytes()
