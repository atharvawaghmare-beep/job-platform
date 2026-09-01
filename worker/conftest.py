"""Present so pytest adds `worker/` (this file's directory) to sys.path,
letting tests do `from backoff import ...` / `from handlers import ...`
regardless of cwd.
"""
