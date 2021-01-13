# peermaps-ingest

Converts OSM data into the peermaps on-disk format.

This is done in two passes of the data. The first pass is to denormalize the
pbf file into an on-disk format that can be easily referenced by external
applications. A second pass enumerates all nodes and writes them to an [eyros
db](https://github.com/peermaps/eyros).

## Usage

```rust
use peermaps_ingest;

let pbf = "/path/to/my/file.pbf";
let output_dir = "denormalized";
let eyros_db = "peermaps.db";

peermaps_ingest::denormalize(pbf, output_dir);
peermaps_ingest::write_to_db(output_dir, eyros_db);
```


## API 

### `ingest::Writer`

Writes osm objects to the on-disk format. 

```rust
use peermaps_ingest;
use vadeen_osm::*;

let mut writer = ingest::Writer::new(output);

let node = Node {
    id: 1,
    coordinate: (66.29, -3.177).into(),
    meta: Meta {
        tags: vec![("key", "value").into()],
        version: Some(3),
        author: Some(AuthorInformation {
            created: 12345678,
            change_set: 1,
            uid: 1234,
            user: "Username".to_string(),
        }),
    },
};


writer.add_node(node)
```

## Development 

For integration tests

```
cargo run /path/to/my/osm.pbf output/
```

To run the unit tests:

```
cargo test
```


