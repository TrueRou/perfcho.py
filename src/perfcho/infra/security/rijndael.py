"""Implement the legacy Rijndael-256-CBC primitive required by osu! Stable.

This module is derived from py3rijndael 0.3.3:
https://github.com/meyt/py3rijndael

MIT License

Copyright (c) 2017 Meyti

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

_BLOCK_SIZE = 32
_ROUNDS = 14
_COLUMNS = _BLOCK_SIZE // 4
_ENCRYPTION_SHIFTS = (1, 3, 4)
_DECRYPTION_SHIFTS = (7, 5, 4)

_AFFINE_MATRIX = (
    (1, 1, 1, 1, 1, 0, 0, 0),
    (0, 1, 1, 1, 1, 1, 0, 0),
    (0, 0, 1, 1, 1, 1, 1, 0),
    (0, 0, 0, 1, 1, 1, 1, 1),
    (1, 0, 0, 0, 1, 1, 1, 1),
    (1, 1, 0, 0, 0, 1, 1, 1),
    (1, 1, 1, 0, 0, 0, 1, 1),
    (1, 1, 1, 1, 0, 0, 0, 1),
)
_AFFINE_CONSTANT = (0, 1, 1, 0, 0, 0, 1, 1)
_MIX_COLUMNS = (
    (2, 1, 1, 3),
    (3, 2, 1, 1),
    (1, 3, 2, 1),
    (1, 1, 3, 2),
)

_ALOG = [1]
for _ in range(255):
    _value = (_ALOG[-1] << 1) ^ _ALOG[-1]
    if _value & 0x100:
        _value ^= 0x11B
    _ALOG.append(_value)

_LOG = [0] * 256
for _index in range(1, 255):
    _LOG[_ALOG[_index]] = _index


def _multiply(left: int, right: int) -> int:
    if left == 0 or right == 0:
        return 0
    return _ALOG[(_LOG[left & 0xFF] + _LOG[right & 0xFF]) % 255]


_inverse_values = [[0] * 8 for _ in range(256)]
_inverse_values[1][7] = 1
for _index in range(2, 256):
    _value = _ALOG[255 - _LOG[_index]]
    for _bit in range(8):
        _inverse_values[_index][_bit] = (_value >> (7 - _bit)) & 1

_affine_values = [[0] * 8 for _ in range(256)]
for _index in range(256):
    for _bit in range(8):
        _affine_values[_index][_bit] = _AFFINE_CONSTANT[_bit]
        for _column in range(8):
            _affine_values[_index][_bit] ^= _AFFINE_MATRIX[_bit][_column] * _inverse_values[_index][_column]

_S_BOX = [0] * 256
_INVERSE_S_BOX = [0] * 256
for _index in range(256):
    _value = _affine_values[_index][0] << 7
    for _bit in range(1, 8):
        _value ^= _affine_values[_index][_bit] << (7 - _bit)
    _S_BOX[_index] = _value
    _INVERSE_S_BOX[_value & 0xFF] = _index

_augmented_mix = [[0] * 8 for _ in range(4)]
for _row in range(4):
    for _column in range(4):
        _augmented_mix[_row][_column] = _MIX_COLUMNS[_row][_column]
        _augmented_mix[_row][_row + 4] = 1

for _row in range(4):
    _pivot = _augmented_mix[_row][_row]
    for _column in range(8):
        if _augmented_mix[_row][_column] != 0:
            _augmented_mix[_row][_column] = _ALOG[
                (255 + _LOG[_augmented_mix[_row][_column] & 0xFF] - _LOG[_pivot & 0xFF]) % 255
            ]
    for _other_row in range(4):
        if _row == _other_row:
            continue
        for _column in range(_row + 1, 8):
            _augmented_mix[_other_row][_column] ^= _multiply(
                _augmented_mix[_row][_column],
                _augmented_mix[_other_row][_row],
            )
        _augmented_mix[_other_row][_row] = 0

_INVERSE_MIX_COLUMNS = tuple(tuple(_augmented_mix[row][column + 4] for column in range(4)) for row in range(4))


def _multiply_word(value: int, factors: tuple[int, ...]) -> int:
    if value == 0:
        return 0
    result = 0
    for factor in factors:
        result <<= 8
        if factor != 0:
            result |= _multiply(value, factor)
    return result


_T1: list[int] = []
_T2: list[int] = []
_T3: list[int] = []
_T4: list[int] = []
_T5: list[int] = []
_T6: list[int] = []
_T7: list[int] = []
_T8: list[int] = []
_U1: list[int] = []
_U2: list[int] = []
_U3: list[int] = []
_U4: list[int] = []

for _index in range(256):
    _substituted = _S_BOX[_index]
    _T1.append(_multiply_word(_substituted, _MIX_COLUMNS[0]))
    _T2.append(_multiply_word(_substituted, _MIX_COLUMNS[1]))
    _T3.append(_multiply_word(_substituted, _MIX_COLUMNS[2]))
    _T4.append(_multiply_word(_substituted, _MIX_COLUMNS[3]))

    _inverse_substituted = _INVERSE_S_BOX[_index]
    _T5.append(_multiply_word(_inverse_substituted, _INVERSE_MIX_COLUMNS[0]))
    _T6.append(_multiply_word(_inverse_substituted, _INVERSE_MIX_COLUMNS[1]))
    _T7.append(_multiply_word(_inverse_substituted, _INVERSE_MIX_COLUMNS[2]))
    _T8.append(_multiply_word(_inverse_substituted, _INVERSE_MIX_COLUMNS[3]))

    _U1.append(_multiply_word(_index, _INVERSE_MIX_COLUMNS[0]))
    _U2.append(_multiply_word(_index, _INVERSE_MIX_COLUMNS[1]))
    _U3.append(_multiply_word(_index, _INVERSE_MIX_COLUMNS[2]))
    _U4.append(_multiply_word(_index, _INVERSE_MIX_COLUMNS[3]))

_ROUND_CONSTANTS = [1]
for _ in range(1, 30):
    _ROUND_CONSTANTS.append(_multiply(2, _ROUND_CONSTANTS[-1]))


class _Rijndael256:
    __slots__ = ("_decryption_keys", "_encryption_keys")

    def __init__(self, key: bytes) -> None:
        if len(key) != _BLOCK_SIZE:
            raise ValueError("Rijndael-256 key must contain exactly 32 bytes")

        encryption_keys = [[0] * _COLUMNS for _ in range(_ROUNDS + 1)]
        decryption_keys = [[0] * _COLUMNS for _ in range(_ROUNDS + 1)]
        round_key_count = (_ROUNDS + 1) * _COLUMNS
        temporary_key = [int.from_bytes(key[offset : offset + 4], "big") for offset in range(0, len(key), 4)]

        target = 0
        source = 0
        while source < _COLUMNS and target < round_key_count:
            encryption_keys[target // _COLUMNS][target % _COLUMNS] = temporary_key[source]
            decryption_keys[_ROUNDS - (target // _COLUMNS)][target % _COLUMNS] = temporary_key[source]
            source += 1
            target += 1

        round_constant_index = 0
        while target < round_key_count:
            value = temporary_key[-1]
            temporary_key[0] ^= (
                (_S_BOX[(value >> 16) & 0xFF] << 24)
                ^ (_S_BOX[(value >> 8) & 0xFF] << 16)
                ^ (_S_BOX[value & 0xFF] << 8)
                ^ _S_BOX[(value >> 24) & 0xFF]
                ^ (_ROUND_CONSTANTS[round_constant_index] << 24)
            )
            round_constant_index += 1
            for index in range(1, _COLUMNS // 2):
                temporary_key[index] ^= temporary_key[index - 1]
            value = temporary_key[_COLUMNS // 2 - 1]
            temporary_key[_COLUMNS // 2] ^= (
                _S_BOX[value & 0xFF]
                ^ (_S_BOX[(value >> 8) & 0xFF] << 8)
                ^ (_S_BOX[(value >> 16) & 0xFF] << 16)
                ^ (_S_BOX[(value >> 24) & 0xFF] << 24)
            )
            for index in range(_COLUMNS // 2 + 1, _COLUMNS):
                temporary_key[index] ^= temporary_key[index - 1]

            source = 0
            while source < _COLUMNS and target < round_key_count:
                encryption_keys[target // _COLUMNS][target % _COLUMNS] = temporary_key[source]
                decryption_keys[_ROUNDS - (target // _COLUMNS)][target % _COLUMNS] = temporary_key[source]
                source += 1
                target += 1

        for round_index in range(1, _ROUNDS):
            for column in range(_COLUMNS):
                value = decryption_keys[round_index][column]
                decryption_keys[round_index][column] = (
                    _U1[(value >> 24) & 0xFF] ^ _U2[(value >> 16) & 0xFF] ^ _U3[(value >> 8) & 0xFF] ^ _U4[value & 0xFF]
                )

        self._encryption_keys = encryption_keys
        self._decryption_keys = decryption_keys

    def encrypt_block(self, block: bytes) -> bytes:
        if len(block) != _BLOCK_SIZE:
            raise ValueError("Rijndael-256 block must contain exactly 32 bytes")

        state = [
            int.from_bytes(block[offset : offset + 4], "big") ^ self._encryption_keys[0][column]
            for column, offset in enumerate(range(0, len(block), 4))
        ]
        transformed = [0] * _COLUMNS
        shift1, shift2, shift3 = _ENCRYPTION_SHIFTS
        for round_index in range(1, _ROUNDS):
            for column in range(_COLUMNS):
                transformed[column] = (
                    _T1[(state[column] >> 24) & 0xFF]
                    ^ _T2[(state[(column + shift1) % _COLUMNS] >> 16) & 0xFF]
                    ^ _T3[(state[(column + shift2) % _COLUMNS] >> 8) & 0xFF]
                    ^ _T4[state[(column + shift3) % _COLUMNS] & 0xFF]
                    ^ self._encryption_keys[round_index][column]
                )
            state = transformed.copy()

        result = bytearray()
        for column in range(_COLUMNS):
            round_key = self._encryption_keys[_ROUNDS][column]
            result.extend(
                (
                    (_S_BOX[(state[column] >> 24) & 0xFF] ^ (round_key >> 24)) & 0xFF,
                    (_S_BOX[(state[(column + shift1) % _COLUMNS] >> 16) & 0xFF] ^ (round_key >> 16)) & 0xFF,
                    (_S_BOX[(state[(column + shift2) % _COLUMNS] >> 8) & 0xFF] ^ (round_key >> 8)) & 0xFF,
                    (_S_BOX[state[(column + shift3) % _COLUMNS] & 0xFF] ^ round_key) & 0xFF,
                )
            )
        return bytes(result)

    def decrypt_block(self, block: bytes) -> bytes:
        if len(block) != _BLOCK_SIZE:
            raise ValueError("Rijndael-256 block must contain exactly 32 bytes")

        state = [
            int.from_bytes(block[offset : offset + 4], "big") ^ self._decryption_keys[0][column]
            for column, offset in enumerate(range(0, len(block), 4))
        ]
        transformed = [0] * _COLUMNS
        shift1, shift2, shift3 = _DECRYPTION_SHIFTS
        for round_index in range(1, _ROUNDS):
            for column in range(_COLUMNS):
                transformed[column] = (
                    _T5[(state[column] >> 24) & 0xFF]
                    ^ _T6[(state[(column + shift1) % _COLUMNS] >> 16) & 0xFF]
                    ^ _T7[(state[(column + shift2) % _COLUMNS] >> 8) & 0xFF]
                    ^ _T8[state[(column + shift3) % _COLUMNS] & 0xFF]
                    ^ self._decryption_keys[round_index][column]
                )
            state = transformed.copy()

        result = bytearray()
        for column in range(_COLUMNS):
            round_key = self._decryption_keys[_ROUNDS][column]
            result.extend(
                (
                    (_INVERSE_S_BOX[(state[column] >> 24) & 0xFF] ^ (round_key >> 24)) & 0xFF,
                    (_INVERSE_S_BOX[(state[(column + shift1) % _COLUMNS] >> 16) & 0xFF] ^ (round_key >> 16)) & 0xFF,
                    (_INVERSE_S_BOX[(state[(column + shift2) % _COLUMNS] >> 8) & 0xFF] ^ (round_key >> 8)) & 0xFF,
                    (_INVERSE_S_BOX[state[(column + shift3) % _COLUMNS] & 0xFF] ^ round_key) & 0xFF,
                )
            )
        return bytes(result)


class Rijndael256Cbc:
    """Encrypt and decrypt Rijndael-256-CBC payloads with strict PKCS#7 padding."""

    __slots__ = ("_cipher", "_iv")

    def __init__(self, key: bytes, iv: bytes) -> None:
        """Bind one 256-bit key and one 256-bit initialization vector."""
        if len(iv) != _BLOCK_SIZE:
            raise ValueError("Rijndael-256-CBC IV must contain exactly 32 bytes")
        self._cipher = _Rijndael256(key)
        self._iv = iv

    def encrypt(self, plaintext: bytes) -> bytes:
        """Pad and encrypt one plaintext payload."""
        padding_size = _BLOCK_SIZE - len(plaintext) % _BLOCK_SIZE
        padded = plaintext + bytes((padding_size,)) * padding_size
        previous = self._iv
        ciphertext = bytearray()
        for offset in range(0, len(padded), _BLOCK_SIZE):
            block = bytes(
                left ^ right for left, right in zip(padded[offset : offset + _BLOCK_SIZE], previous, strict=True)
            )
            previous = self._cipher.encrypt_block(block)
            ciphertext.extend(previous)
        return bytes(ciphertext)

    def decrypt(self, ciphertext: bytes) -> bytes:
        """Decrypt and strictly remove PKCS#7 padding from one payload."""
        if not ciphertext or len(ciphertext) % _BLOCK_SIZE:
            raise ValueError("Rijndael-256-CBC ciphertext must contain complete 32-byte blocks")
        previous = self._iv
        plaintext = bytearray()
        for offset in range(0, len(ciphertext), _BLOCK_SIZE):
            block = ciphertext[offset : offset + _BLOCK_SIZE]
            decrypted = self._cipher.decrypt_block(block)
            plaintext.extend(left ^ right for left, right in zip(decrypted, previous, strict=True))
            previous = block

        padding_size = plaintext[-1]
        if not 1 <= padding_size <= _BLOCK_SIZE or plaintext[-padding_size:] != bytes((padding_size,)) * padding_size:
            raise ValueError("Rijndael-256-CBC payload has invalid PKCS#7 padding")
        return bytes(plaintext[:-padding_size])
