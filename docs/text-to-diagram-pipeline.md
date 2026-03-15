# Text-to-Diagram Pipeline: Database-First Architecture

> Captured from a Claude app conversation. Not yet implemented in CLI.

## Background

Long-time Graphviz/DOT user (since early 90s) exploring modern text-to-diagram tools. Just discovered D2 (by Terrastruct). Also familiar with Mermaid but hasn't used it heavily.

## Key Interests

- Textual/declarative graph specification that can be rendered to SVG/PNG
- Usable as a library, embeddable as a component in other systems
- Generating diagrams from JSON data fed into templates
- "Database-first" / metadata-driven architecture

## Technology Stack

- **DuckDB** as the primary environment
- **Python integration** via DuckDB UDFs (registered Python callables become SQL functions)
- Driving everything from SQL queries against metadata/data tables

## The Architecture

**Kroki** (self-hosted via Docker) as the rendering backend — a single HTTP endpoint that accepts diagram source text and returns SVG, supporting D2, Graphviz, Mermaid, Vega-Lite, PlantUML and many others. The generality is the point: one pipeline, multiple rendering engines selected per diagram type.

### Four-Layer Model

| Layer | Description |
|-------|-------------|
| **Metadata** | Tables for nodes, edges, diagram types, templates |
| **Assembly** | SQL views/macros that produce diagram source text or Vega-Lite JSON specs |
| **Render** | Python UDFs (`kroki_render()` etc.) registered into DuckDB, calling self-hosted Kroki |
| **Cache** | Rendered SVGs stored keyed by hash of source |

### Example Metadata Schema

```sql
CREATE TABLE diagram_type (
    name        TEXT PRIMARY KEY,   -- 'dependency_graph', 'er_diagram', etc.
    engine      TEXT,               -- 'd2', 'graphviz', 'vega-lite'
    template    TEXT                -- source template with {placeholders}
);

CREATE TABLE diagram_spec (
    id          TEXT PRIMARY KEY,
    type        TEXT REFERENCES diagram_type(name),
    params      JSON,               -- parameterizes the template/query
    source_sql  TEXT                -- query that produces the diagram source text
);
```

### Vega-Lite Direct from SQL

Vega-Lite's spec is JSON, and DuckDB's JSON functions can build it directly from query results — no separate templating language needed. SQL IS the template engine:

```sql
SELECT json_object(
    '$schema', 'https://vega.github.io/schema/vega-lite/v5.json',
    'mark',    'bar',
    'encoding', json_object(
        'x', json_object('field', 'category', 'type', 'nominal'),
        'y', json_object('field', 'value',    'type', 'quantitative')
    ),
    'data', json_object(
        'values', json_group_array(json_object('category', cat, 'value', val))
    )
) AS vega_spec
FROM my_data;
```

### Rendering Pattern (Bulk/Pipeline)

```sql
-- queue diagrams that need rendering
INSERT INTO render_queue SELECT id FROM diagram_spec WHERE svg IS NULL;

-- Python process drains the queue, calls Kroki, writes SVGs back
-- using duckdb.connect() from Python rather than Python called from DuckDB
```

## Key Tool Notes

- **D2**: Go library, clean programmatic API (d2oracle), beautiful SVG output, pluggable layout engines (dagre, ELK, TALA). Not natively supported in GitHub Markdown.
- **Mermaid**: JavaScript, renders natively in GitHub/GitLab Markdown. Best for sequence diagrams and docs-as-code in git platforms.
- **Graphviz**: Still the best for algorithmic layout control and large graphs. DOT language. D2 can use Graphviz as a layout backend.
- **Vega-Lite**: Takes JSON spec directly — best for tabular/statistical data visualization rather than graph topology.
- **Kroki**: Unified HTTP API wrapping all of the above plus many more. Self-host with Docker.

## Connection Points

- **Kroki rendering via blobhttp**: `http_post(kroki_url, body := diagram_source)` — the render call is just another HTTP scalar function. No Python UDF needed if we use blobhttp directly.
- **Diagram source via blobtemplates**: `template_render(d2_template, json_object('nodes', ..., 'edges', ...))` generates D2/DOT/Mermaid source text from metadata.
- **ER diagrams from blobapi catalog**: The `api_spec`/`api_path`/`api_operation` tables are a natural source for API dependency graphs and schema diagrams.
- **Vega-Lite from blobapi weather data**: The HDD time-series data is a direct Vega-Lite use case — `json_object()` builds the spec, Kroki renders it.

## Open Question

What specific domain is this for? (infrastructure diagrams, data lineage, ER diagrams from schema, something else?) — this determines which Kroki engines and template patterns to prioritize first.
