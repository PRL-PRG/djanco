# Djanco mk1

How to build:

```
cargo build --color=always --all --all-targets
```

How to run queries:

```
cargo run --bin example --release -- -o /dejacode/query_results -d /dejacode/dataset -c /dejacode/query_results/cache --data-dump=/dejacode/query_results/dump
```

Usage

```
cargo run --bin example -- --help
```

```
USAGE:
    example [OPTIONS] --dataset <DATASET_PATH> --output <OUTPUT_PATH>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -d, --dataset <DATASET_PATH>           
        --data-dump <DATA_DUMP_PATH>       
    -o, --output <OUTPUT_PATH>             
    -c, --cache <PERSISTENT_CACHE_PATH> 
```