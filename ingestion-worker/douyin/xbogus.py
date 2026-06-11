from __future__ import annotations

import base64
import hashlib
import random
import time

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


def generate_random_str(length: int) -> str:
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789="
    return "".join(random.choice(chars) for _ in range(length))


class XBogus:
    def sign(self, payload: str, form: str = "", user_agent: str = USER_AGENT) -> str:
        xbogus = self._get_xbogus(payload, user_agent, form)
        return f"{payload}&X-Bogus={xbogus}"

    def _get_xbogus(self, payload: str, user_agent: str, form: str) -> str:
        chars = "Dkdpgh4ZKsQB80/Mfvw36XI1R25-WUAlEi7NLboqYTOPuzmFjJnryx9HVGcaStCe="
        arr2 = self._get_arr2(payload, user_agent, form)
        garbled = self._get_garbled_string(arr2)
        xbogus = ""

        for index in range(0, 21, 3):
            char0 = garbled[index]
            char1 = garbled[index + 1]
            char2 = garbled[index + 2]
            base = char2 | char1 << 8 | char0 << 16
            xbogus += chars[(base & 16515072) >> 18]
            xbogus += chars[(base & 258048) >> 12]
            xbogus += chars[(base & 4032) >> 6]
            xbogus += chars[base & 63]

        return xbogus

    def _get_garbled_string(self, arr2: list[int]) -> list[int]:
        pattern = [
            arr2[0], arr2[10], arr2[1], arr2[11], arr2[2], arr2[12], arr2[3], arr2[13], arr2[4], arr2[14],
            arr2[5], arr2[15], arr2[6], arr2[16], arr2[7], arr2[17], arr2[8], arr2[18], arr2[9],
        ]
        char_array = [chr(item) for item in pattern]
        result = [2, 255]
        result.extend(self._rc4_like(["ÿ"], "".join(char_array)))
        return result

    def _get_arr2(self, payload: str, user_agent: str, form: str) -> list[int]:
        salt_payload = list(hashlib.md5(hashlib.md5(payload.encode()).digest()).digest())
        salt_form = list(hashlib.md5(hashlib.md5(form.encode()).digest()).digest())
        ua_key = ["\u0000", "\u0001", "\u000e"]
        salt_ua = list(hashlib.md5(base64.b64encode(self._rc4_like(ua_key, user_agent))).digest())
        timestamp = int(time.time())
        canvas = 1489154074

        arr1 = [
            64, 0, 1, 14, salt_payload[14], salt_payload[15], salt_form[14], salt_form[15], salt_ua[14], salt_ua[15],
            (timestamp >> 24) & 255, (timestamp >> 16) & 255, (timestamp >> 8) & 255, timestamp & 255,
            (canvas >> 24) & 255, (canvas >> 16) & 255, (canvas >> 8) & 255, canvas & 255, 64,
        ]

        for index in range(1, len(arr1) - 1):
            arr1[18] ^= arr1[index]

        return [
            arr1[0], arr1[2], arr1[4], arr1[6], arr1[8], arr1[10], arr1[12], arr1[14], arr1[16], arr1[18],
            arr1[1], arr1[3], arr1[5], arr1[7], arr1[9], arr1[11], arr1[13], arr1[15], arr1[17],
        ]

    @staticmethod
    def _rc4_like(key: list[str], text: str) -> bytearray:
        state = [i for i in range(256)]
        cursor = 0
        result = bytearray(len(text))

        for index in range(256):
            cursor = (cursor + state[index] + ord(key[index % len(key)])) % 256
            state[index], state[cursor] = state[cursor], state[index]

        t = 0
        cursor = 0
        for index in range(len(text)):
            t = (t + 1) % 256
            cursor = (cursor + state[t]) % 256
            state[t], state[cursor] = state[cursor], state[t]
            result[index] = ord(text[index]) ^ state[(state[t] + state[cursor]) % 256]

        return result