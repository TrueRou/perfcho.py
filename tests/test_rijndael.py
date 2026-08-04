import pytest

from perfcho.infra.security.rijndael import Rijndael256Cbc

_KEY = bytes(range(32))
_IV = bytes(range(32, 64))
_PLAINTEXT = b"perfcho stable rijndael-256"
_CIPHERTEXT = bytes.fromhex("dea0665772628bd9bde9d8f4a6ec9f51b6983bf26a4c209a2f091b81df8a5c1f")


def test_rijndael_256_cbc_matches_py3rijndael_0_3_3_vector() -> None:
    cipher = Rijndael256Cbc(_KEY, _IV)

    assert cipher.encrypt(_PLAINTEXT) == _CIPHERTEXT
    assert cipher.decrypt(_CIPHERTEXT) == _PLAINTEXT


def test_rijndael_256_cbc_rejects_invalid_sizes_and_padding() -> None:
    with pytest.raises(ValueError, match="key"):
        Rijndael256Cbc(b"short", _IV)
    with pytest.raises(ValueError, match="IV"):
        Rijndael256Cbc(_KEY, b"short")

    cipher = Rijndael256Cbc(_KEY, _IV)
    with pytest.raises(ValueError, match="complete"):
        cipher.decrypt(b"")
    with pytest.raises(ValueError, match="padding"):
        cipher.decrypt(_CIPHERTEXT[:-1] + bytes((_CIPHERTEXT[-1] ^ 1,)))
