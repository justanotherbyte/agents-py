"""Splitting message JSON across rows (upstream ``sessions/chunking.ts``)."""

__all__ = ("MAX_INLINE_ROW_BYTES", "split_content")

MAX_INLINE_ROW_BYTES = 1536 * 1024
"""UTF-8 bytes of message JSON per row (under the 2 MiB row limit)."""


def split_content(text: str, budget: int = MAX_INLINE_ROW_BYTES) -> list[str]:
    """Cut ``text`` into slices of at most ``budget`` UTF-8 bytes.

    Cuts fall between characters, so ``"".join(slices) == text``. A single
    character wider than the budget gets a slice of its own.
    """
    data = text.encode("utf-8", "surrogatepass")
    if len(data) <= budget:
        return [text]
    slices: list[str] = []
    start = 0
    while start < len(data):
        end = min(start + budget, len(data))
        # Back up off continuation bytes (0b10xxxxxx) to a character boundary.
        while end < len(data) and end > start and data[end] & 0xC0 == 0x80:
            end -= 1
        if end == start:  # one character wider than the budget
            end += 1
            while end < len(data) and data[end] & 0xC0 == 0x80:
                end += 1
        slices.append(data[start:end].decode("utf-8", "surrogatepass"))
        start = end
    return slices
