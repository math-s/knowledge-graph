# Sum 1..N in parallel to verify the toolchain.

def sum(start, end):
  if start == end:
    return start
  else:
    mid = (start + end) / 2
    left = sum(start, mid)
    right = sum(mid + 1, end)
    return left + right

def main():
  return sum(1, 1000)
