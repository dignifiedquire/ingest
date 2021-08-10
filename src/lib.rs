#![feature(backtrace)]

pub mod encoder;
pub use encoder::*;
pub mod error;
pub use error::*;
mod record;
use osmxq::{XQ,Record};
mod value;
mod progress;
pub use progress::Progress;
use rayon::prelude::*;

pub const BACKREF_PREFIX: u8 = 1;
pub const REF_PREFIX: u8 = 2;

use std::collections::HashMap;
use std::{sync::{Arc,Mutex,RwLock}};
use crossbeam_channel as channel;

type NodeDeps = HashMap<u64,(f32,f32)>;
type WayDeps = HashMap<u64,Vec<u64>>;

type R = Decoded;
type T = eyros::Tree2<f32,f32,V>;
type P = (eyros::Coord<f32>,eyros::Coord<f32>);
type V = value::V;
pub type EDB = eyros::DB<random_access_disk::RandomAccessDisk,T,P,V>;

pub struct Ingest<S> where S: osmxq::RW {
  xq: Arc<Mutex<XQ<S,R>>>,
  db: Arc<Mutex<EDB>>,
  place_other: u64,
  pub progress: Arc<RwLock<Progress>>,
}

impl<S> Ingest<S> where S: osmxq::RW+'static {
  pub fn new(xq: XQ<S,R>, db: EDB, stages: &[&str]) -> Self {
    Self {
      xq: Arc::new(Mutex::new(xq)),
      db: Arc::new(Mutex::new(db)),
      place_other: *georender_pack::osm_types::get_types().get("place.other").unwrap(),
      progress: Arc::new(RwLock::new(Progress::new(stages))),
    }
  }
  pub fn load_pbf(&mut self, pbf: std::path::PathBuf) -> Result<(),Error> {
    self.progress.write().unwrap().start("pbf");
    let (sender,receiver) = channel::bounded::<Decoded>(1_000);

    std::thread::spawn(move || {
        let reader = unsafe { osmpbf::mmap_blob::Mmap::from_path(pbf) }.unwrap();
        if let Err(err) = reader.blob_iter()
            .try_for_each(move |blob| {
                use osmpbf::blob::BlobDecode;
                
                match blob?.decode() {
                    Ok(BlobDecode::OsmHeader(_)) | Ok(BlobDecode::Unknown(_)) => {}
                    Ok(BlobDecode::OsmData(block)) => {
                        let sender = sender.clone();
                        block.for_each_element(move |element| {
                            let r = Decoded::from_pbf_element(&element).unwrap();
                            sender.send(r).unwrap();
                        });
                                }
                    Err(e) => return Err(e),
                }
                Ok(())
            }) {
                eprintln!("failed reader: {:?}", err);
            }

    });

    const BATCH_SIZE: usize = 50_000;
    const NUM_WORKERS: usize = 4;
    let mut workers = Vec::with_capacity(NUM_WORKERS);
    for _ in 0..NUM_WORKERS {
        let progress = self.progress.clone();
        let xq = self.xq.clone();
        let receiver = receiver.clone();
        
        workers.push(std::thread::spawn(move || {
            let mut records = Vec::with_capacity(BATCH_SIZE);      

            while let Ok(record) = receiver.recv() {
                records.push(record);
                if records.len() >= BATCH_SIZE {
                    let mut xq = xq.lock().unwrap();
                    if let Err(err) = xq.add_records(&records) {
                        progress.write().unwrap().push_err("pbf", &err);
                    } else {
                        progress.write().unwrap().add("pbf", records.len());
                    }
                    records.clear();
                }
            }
            if !records.is_empty() {
                let mut xq = xq.lock().unwrap();
                let res = xq.add_records(&records);

                if let Err(err) = res {
                    progress.write().unwrap().push_err("pbf", &err);
                }
                progress.write().unwrap().add("pbf", records.len());
            }
        }));
    }

    for worker in workers.into_iter() {
        worker.join().unwrap();
    }

    {
        let mut xq = self.xq.lock().unwrap();
        xq.finish().unwrap();
        xq.flush().unwrap();
    }

    self.progress.write().unwrap().end("pbf");

    Ok(())
  }

  // loop over the db, denormalize the records, georender-pack the data into eyros
  pub fn process(&mut self) -> () {
    self.progress.write().unwrap().start("process");
    let mut xq = self.xq.lock().unwrap();
    let quad_ids = xq.get_quad_ids();
    for q_id in quad_ids {
      let records = xq.read_quad_denorm(q_id).unwrap();
      let rlen = records.len();
      let mut batch = Vec::with_capacity(records.len());
      for (_r_id,r,deps) in records {
        match &r {
          Decoded::Node(node) => {
            if node.feature_type == self.place_other { continue }
            let r_encoded = georender_pack::encode::node_from_parsed(
              node.id*3+0, (node.lon,node.lat), node.feature_type, &node.labels
            );
            if let Ok(encoded) = r_encoded {
              if encoded.is_empty() { continue }
              batch.push(eyros::Row::Insert(
                (eyros::Coord::Scalar(node.lon),eyros::Coord::Scalar(node.lat)),
                encoded.into()
              ));
            }
          },
          Decoded::Way(way) => {
            if way.feature_type == self.place_other { continue }
            let mut pdeps = HashMap::new();
            for d in deps {
              if let Some(p) = d.get_position() {
                pdeps.insert(d.get_id()/3, p);
              }
            }
            let mut bbox = (f32::INFINITY,f32::INFINITY,f32::NEG_INFINITY,f32::NEG_INFINITY);
            if pdeps.len() <= 1 { continue }
            for (lon,lat) in pdeps.values() {
              bbox.0 = bbox.0.min(*lon);
              bbox.1 = bbox.1.min(*lat);
              bbox.2 = bbox.2.max(*lon);
              bbox.3 = bbox.3.max(*lat);
            }
            let r_encoded = georender_pack::encode::way_from_parsed(
              way.id*3+1, way.feature_type, way.is_area, &way.labels, &way.refs, &pdeps
            );
            if let Ok(encoded) = r_encoded {
              if encoded.is_empty() { continue }
              let point = (
                eyros::Coord::Interval(bbox.0,bbox.2),
                eyros::Coord::Interval(bbox.1,bbox.3),
              );
              batch.push(eyros::Row::Insert(point, encoded.into()));
            }
          },
          Decoded::Relation(relation) => {
            if relation.feature_type == self.place_other { continue }
            let mut node_deps: NodeDeps = HashMap::new();
            let mut way_deps: WayDeps = HashMap::new();

            for d in deps {
              if let Some(p) = d.get_position() {
                node_deps.insert(d.get_id()/3, p);
                continue;
              }
              let drefs = d.get_refs().iter().map(|dr| dr/3).collect::<Vec<u64>>();
              if drefs.is_empty() { continue }
              way_deps.insert(d.get_id()/3, drefs);
            }
            let mut bbox = (f32::INFINITY,f32::INFINITY,f32::NEG_INFINITY,f32::NEG_INFINITY);
            if node_deps.len() <= 1 { continue }
            for p in node_deps.values() {
              bbox.0 = bbox.0.min(p.0);
              bbox.1 = bbox.1.min(p.1);
              bbox.2 = bbox.2.max(p.0);
              bbox.3 = bbox.3.max(p.1);
            }
            let members = relation.members.iter().map(|m| {
              georender_pack::Member::new(
                m/2,
                match m%2 {
                  0 => georender_pack::MemberRole::Outer(),
                  _ => georender_pack::MemberRole::Inner(),
                },
                georender_pack::MemberType::Way()
              )
            }).collect::<Vec<_>>();
            let r_encoded = georender_pack::encode::relation_from_parsed(
              relation.id*3+2, relation.feature_type, relation.is_area,
              &relation.labels, &members, &node_deps, &way_deps
            );
            if let Ok(encoded) = r_encoded {
              let point = (
                eyros::Coord::Interval(bbox.0,bbox.2),
                eyros::Coord::Interval(bbox.1,bbox.3),
              );
              batch.push(eyros::Row::Insert(point, encoded.into()));
            }
          },
        }
      }
      let db = self.db.clone();
      async_std::task::block_on(async move {
        let mut db = db.lock().unwrap();
        db.batch(&batch).await.unwrap();
      });
      self.progress.write().unwrap().add("process", rlen);
    }

    let db = self.db.clone();
    async_std::task::block_on(async move {
      let mut db = db.lock().unwrap();
      db.sync().await.unwrap();
    });
    self.progress.write().unwrap().end("process");
  }
}
