//! Receiver operations for the shared typed publication engine.
use super::*;

pub(super) enum ReceiverMaterial<'a> {
    Refresh {
        serving: &'a Publications,
        anchor: (u32, [u8; 32]),
        epoch: u64,
        report: serde_json::Value,
    },
    Prepared(Prepared<'a>),
}
pub(super) struct ReceiverPublication<'a> {
    pub(super) fence: Option<(&'a Publications, u64)>,
    pub(super) work: Option<Collected<'a>>,
    pub(super) state: Option<&'a mut State>,
    pub(super) report: Option<serde_json::Value>,
    pub(super) refresh: bool,
}
impl<'a> pir_pipeline::publisher::Publication for ReceiverPublication<'a> {
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Material = ReceiverMaterial<'a>;
    type Stored = ReceiverMaterial<'a>;
    type Verified = ReceiverMaterial<'a>;
    type Candidate = ReceiverMaterial<'a>;
    type Report = ();
    fn check(&self) -> Result<()> {
        if self
            .fence
            .is_some_and(|(owner, epoch)| owner.epoch() != epoch)
        {
            return Err("publication invalidated during preparation; retrying".into());
        }
        Ok(())
    }
    async fn build(&mut self) -> Result<Self::Material> {
        let work = self.work.take().expect("publication is built once");
        let report = self.report.take().expect("audit captured at selection");
        let state = self
            .state
            .take()
            .expect("publication owns preparation state");
        if self.refresh {
            Ok(ReceiverMaterial::Refresh {
                serving: work.serving.expect("live refresh"),
                anchor: (work.tip.height, work.tip.hash),
                epoch: work.epoch.expect("live fence"),
                report,
            })
        } else {
            Ok(ReceiverMaterial::Prepared(
                work.prepare(state, report).await?,
            ))
        }
    }
    async fn store(&mut self, mut material: Self::Material) -> Result<Self::Stored> {
        if let ReceiverMaterial::Prepared(prepared) = &mut material {
            if let Candidate::Files(layout) = &prepared.candidate {
                let started = std::time::Instant::now();
                let revision = write_layout(&prepared.args.data_dir.join("publications"), layout)?;
                prepared.summary["revision"] = hex::encode(revision).into();
                log_stage("directory_write", started);
            }
        }
        // Live immutable tables are stored a shard at a time during preparation,
        // preserving the product's bounded memory and durable reuse policy.
        Ok(material)
    }
    async fn verify(&mut self, stored: Self::Stored) -> Result<Self::Verified> {
        // Preparation establishes payment, witness and filter consistency as each
        // shard is sealed. It does not reopen the same tables for another hash pass.
        Ok(stored)
    }
    async fn assemble(&mut self, mut verified: Self::Verified) -> Result<Self::Candidate> {
        if let ReceiverMaterial::Prepared(prepared) = &mut verified {
            if let Candidate::Unassembled {
                genesis,
                revision,
                shards,
            } = &mut prepared.candidate
            {
                let publication =
                    Publication::assemble(*genesis, *revision, std::mem::take(shards))?;
                prepared.revision = hex::encode(publication.id());
                prepared.candidate = Candidate::Serving(Box::new(publication));
            }
        }
        Ok(verified)
    }
    async fn recheck(&mut self, candidate: &mut Self::Candidate) -> Result<()> {
        if let ReceiverMaterial::Prepared(prepared) = candidate {
            if let Candidate::Serving(publication) = &prepared.candidate {
                let anchor = publication.head();
                if !is_canonical(prepared.rpc, anchor.end_height, anchor.end_hash).await? {
                    prepared.serving.expect("live candidate").revoke();
                    return Err("chain changed during PIR preparation; retrying".into());
                }
            }
        }
        Ok(())
    }
    async fn commit(self, candidate: Self::Candidate) -> Result<()> {
        match candidate {
            ReceiverMaterial::Refresh {
                serving,
                anchor,
                epoch,
                report,
            } => {
                if !serving.update_report(anchor, report, epoch) {
                    return Err("publication invalidated during the audit refresh; retrying".into());
                }
            }
            ReceiverMaterial::Prepared(Prepared {
                candidate,
                serving,
                epoch,
                active,
                history_revision,
                report,
                refresh_started,
                revision,
                summary,
                ..
            }) => match candidate {
                Candidate::Unassembled { .. } => {
                    unreachable!("the engine assembles before committing")
                }
                Candidate::Files(_) => println!("{summary}"),
                Candidate::Serving(publication) => {
                    let started = std::time::Instant::now();
                    let head = publication.head();
                    let anchor = (head.end_height, head.end_hash);
                    if !serving
                        .expect("live candidate")
                        .publish(publication.with_report(report), epoch.unwrap())
                    {
                        return Err("publication invalidated during preparation; retrying".into());
                    }
                    *active = Some(Active {
                        anchor,
                        history_revision,
                    });
                    log_stage("activate", started);
                    info!(height = anchor.0, %revision,
                            processing_ms = refresh_started.elapsed().as_millis() as u64,
                            publication = %summary, "receiver publication");
                }
            },
        }
        Ok(())
    }
}
