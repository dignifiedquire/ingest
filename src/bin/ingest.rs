#![feature(backtrace)]
use peermaps_ingest::{Ingest,Key,EStore,LStore,Phase};
use leveldb::{database::Database,options::Options};
use async_std::{io,fs::File};

type Error = Box<dyn std::error::Error+Send+Sync>;

#[async_std::main]
async fn main() -> Result<(),Error> {
  if let Err(err) = run().await {
    match err.backtrace().map(|bt| (bt,bt.status())) {
      Some((bt,std::backtrace::BacktraceStatus::Captured)) => {
        eprint!["{}\n{}", err, bt];
      },
      _ => eprintln!["{}", err],
    }
    std::process::exit(1);
  }
  Ok(())
}

async fn run() -> Result<(),Error> {
  let (args,argv) = argmap::new()
    .booleans(&["help","h"])
    .parse(std::env::args());
  if argv.contains_key("help") || argv.contains_key("h") {
    print!["{}", usage(&args)];
    return Ok(());
  }

  let mut counter: u64 = 0;
  let mut last_print = std::time::Instant::now();
  let mut last_phase: Option<Phase> = None;
  let reporter = Box::new(move |phase: Phase, res| {
    if let Err(e) = res {
      eprintln!["\x1b[1K\r{} error: {}", phase.to_string(), e];
      last_print = std::time::Instant::now();
    } else {
      counter += 1;
      if last_phase.as_ref().and_then(|p| Some(p != &phase)).unwrap_or(false) {
        eprintln!["\x1b[1K\r{} {}", last_phase.as_ref().unwrap().to_string(), counter];
        counter = 1;
        eprint!["{} {}", phase.to_string(), counter];
        last_print = std::time::Instant::now();
      }
      if last_print.elapsed().as_secs_f64() >= 1.0 {
        eprint!["\x1b[1K\r{} {}", phase.to_string(), counter];
        last_print = std::time::Instant::now();
      }
    }
    last_phase = Some(phase);
  });

  match args.get(1).map(|x| x.as_str()) {
    None => print!["{}", usage(&args)],
    Some("help") => print!["{}", usage(&args)],
    Some("ingest") => {
      let stdin_file = "-".to_string();
      let pbf_file = argv.get("pbf").and_then(|x| x.first()).unwrap_or(&stdin_file);
      let ldb_dir = argv.get("ldb").and_then(|x| x.first());
      let edb_dir = argv.get("edb").and_then(|x| x.first());
      if ldb_dir.is_none() || edb_dir.is_none() {
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir.unwrap())).await?)
      ).reporter(reporter);
      let pbf_stream: Box<dyn std::io::Read+Send> = match pbf_file.as_str() {
        "-" => Box::new(std::io::stdin()),
        x => Box::new(std::fs::File::open(x)?),
      };
      ingest.load_pbf(pbf_stream).await?;
      ingest.process().await;
    },
    Some("pbf") => {
      let stdin_file = "-".to_string();
      let pbf_file = argv.get("pbf").and_then(|x| x.first()).unwrap_or(&stdin_file);
      let ldb_dir = argv.get("ldb").and_then(|x| x.first());
      let edb_dir = argv.get("edb").and_then(|x| x.first());
      if ldb_dir.is_none() || edb_dir.is_none() {
        eprint!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir.unwrap())).await?)
      ).reporter(reporter);
      let pbf_stream: Box<dyn std::io::Read+Send> = match pbf_file.as_str() {
        "-" => Box::new(std::io::stdin()),
        x => Box::new(std::fs::File::open(x)?),
      };
      ingest.load_pbf(pbf_stream).await?;
      ingest.process().await;
    },
    Some("process") => {
      let ldb_dir = argv.get("ldb").and_then(|x| x.first());
      let edb_dir = argv.get("edb").and_then(|x| x.first());
      if ldb_dir.is_none() || edb_dir.is_none() {
        eprint!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir.unwrap())).await?)
      ).reporter(reporter);
      ingest.process().await;
    },
    Some("changeset") => {
      let o5c_file = argv.get("o5c").and_then(|x| x.first());
      let ldb_dir = argv.get("ldb").and_then(|x| x.first());
      let edb_dir = argv.get("edb").and_then(|x| x.first());
      if o5c_file.is_none() || ldb_dir.is_none() || edb_dir.is_none() {
        eprint!["{}",usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir.unwrap())).await?)
      ).reporter(reporter);
      let o5c_stream: Box<dyn io::Read+Unpin> = match o5c_file.unwrap().as_str() {
        "-" => Box::new(io::stdin()),
        x => Box::new(File::open(x).await?),
      };
      ingest.changeset(o5c_stream).await?;
    },
    Some(cmd) => {
      eprint!["unrecognized command {}", cmd];
      std::process::exit(1);
    },
  }
  Ok(())
}

fn open(path: &std::path::Path) -> Result<Database<Key>,Error> {
  let mut options = Options::new();
  options.create_if_missing = true;
  options.compression = leveldb_sys::Compression::Snappy;
  Database::open(path, options).map_err(|e| e.into())
}

fn usage(args: &[String]) -> String {
  format![indoc::indoc![r#"usage: {} COMMAND {{OPTIONS}}

    ingest - runs pbf and process phases
      --pbf  osm pbf file to ingest or "-" for stdin (default)
      --ldb  level db dir to write normalized data
      --edb  eyros db dir to write spatial data

    pbf - parse pbf and write normalized data to level db
      --pbf  osm pbf file to ingest or "-" for stdin (default)
      --ldb  level db dir to write normalized data
      --edb  eyros db dir to write spatial data

    process - write georender-pack data to eyros db from populated level db
      --ldb  level db dir to read normalized data
      --edb  eyros db dir to write spatial data

    changeset - ingest data from an o5c changeset
      --o5c  o5c changeset file or "-" for stdin (default)
      --ldb  level db dir to read/write normalized data
      --edb  eyros db dir to write spatial data

  "#], args.get(0).unwrap_or(&"???".to_string())]
}
