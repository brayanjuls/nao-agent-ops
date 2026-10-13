# Option A: add hudi-mcp to the nao image so nao can start it over stdio (agent/mcps/mcp.json "command").
#   docker build --target nao -t my-nao-with-hudi .
# Option B: run hudi-mcp as its own HTTP service (needed for nao cloud mode, which only allows HTTP MCP).
#   docker build --target server -t hudi-mcp .
#   docker run -p 8000:8000 -e HUDI_MCP_CONFIG=/config/config.yaml -v $PWD/config.yaml:/config/config.yaml hudi-mcp
#
# Both bake DuckDB's httpfs / aws / delta extensions into the image, so nothing is downloaded at runtime.

FROM docker.io/library/python:3.12-slim AS server
WORKDIR /opt/hudi-mcp
ENV HUDI_MCP_DUCKDB_EXTENSION_DIRECTORY=/opt/hudi-mcp/duckdb_extensions
COPY pyproject.toml README.md ./
COPY hudi_mcp ./hudi_mcp
RUN pip install --no-cache-dir . \
 && python -c "import duckdb, os; c = duckdb.connect(config={'extension_directory': os.environ['HUDI_MCP_DUCKDB_EXTENSION_DIRECTORY']}); [c.install_extension(e) for e in ('httpfs', 'aws', 'delta')]" \
 && chmod -R a+rX /opt/hudi-mcp/duckdb_extensions
EXPOSE 8000
ENTRYPOINT ["hudi-mcp"]
CMD ["--transport", "streamable-http", "--host", "0.0.0.0", "--port", "8000"]

FROM docker.io/getnao/nao:latest AS nao
ENV HUDI_MCP_DUCKDB_EXTENSION_DIRECTORY=/opt/hudi-mcp/duckdb_extensions
COPY pyproject.toml README.md /opt/hudi-mcp/
COPY hudi_mcp /opt/hudi-mcp/hudi_mcp
# Own virtualenv so its pyarrow/duckdb versions can't clash with nao's Python dependencies.
RUN python3 -m venv /opt/hudi-mcp/venv \
 && /opt/hudi-mcp/venv/bin/pip install --no-cache-dir /opt/hudi-mcp \
 && /opt/hudi-mcp/venv/bin/python -c "import duckdb, os; c = duckdb.connect(config={'extension_directory': os.environ['HUDI_MCP_DUCKDB_EXTENSION_DIRECTORY']}); [c.install_extension(e) for e in ('httpfs', 'aws', 'delta')]" \
 && chmod -R a+rX /opt/hudi-mcp/duckdb_extensions \
 && ln -s /opt/hudi-mcp/venv/bin/hudi-mcp /usr/local/bin/hudi-mcp
