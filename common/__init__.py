"""Shared code between the API and worker processes — currently just the
ORM models, since both sides read/write the same `jobs` table. Kept as its
own top-level package (not nested under api/) so neither service "owns" it;
each Dockerfile copies it in explicitly.
"""
