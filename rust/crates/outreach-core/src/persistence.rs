//! Snapshot-bound, single-use permits. This ledger is an offline persistence
//! boundary, not a production SQLite writer. Services acknowledge the save;
//! only this sealed wrapper can mint a permit after that acknowledgement.
use crate::mathesis_delivery::{
    DeliveryPorts, DeliveryServices, LinkIntent, SourceFile, StageResult,
};
use outreach_canon::{Value, digest};

pub(crate) mod sealed {
    pub trait Sealed {}
}

#[derive(Clone, Debug)]
pub struct SavedSnapshot {
    pub generation: u64,
    pub digest: String,
}

/// Neither constructible, cloneable nor deserializable by an adapter.
/// ```compile_fail
/// use outreach_core::persistence::Persisted;
/// let permit = Persisted { intent: (), generation: 1, snapshot_digest: String::new() };
/// ```
/// A write consumes the permit, including when its outcome is uncertain.
/// ```compile_fail
/// use outreach_core::mathesis_delivery::{MathesisClient, ManuscriptIntent, Persisted};
/// fn reuse(client: &mut impl MathesisClient, permit: Persisted<ManuscriptIntent>) {
///     client.deliver(permit);
///     client.deliver(permit);
/// }
/// ```
pub struct Persisted<I> {
    intent: I,
    generation: u64,
    snapshot_digest: String,
}
impl<I> Persisted<I> {
    pub fn intent(&self) -> &I {
        &self.intent
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn snapshot_digest(&self) -> &str {
        &self.snapshot_digest
    }
    pub(crate) fn saved(
        intent: I,
        ledger: &impl DeliveryPorts,
        tables: &Value,
    ) -> StageResult<Self> {
        let snapshot_digest = digest(tables)?;
        let (saved, logical_digest) = ledger
            .saved_snapshot()
            .ok_or_else(|| outreach_canon::Error::new("No saved snapshot"))?;
        crate::validation::require(
            logical_digest == snapshot_digest,
            "Intent snapshot differs from the saved snapshot",
        )?;
        Ok(Self {
            intent,
            generation: saved.generation,
            snapshot_digest: saved.digest.clone(),
        })
    }
    pub(crate) fn save(
        intent: I,
        ledger: &mut impl DeliveryPorts,
        tables: &Value,
    ) -> StageResult<Self> {
        ledger.save_forced(tables)?;
        Self::saved(intent, ledger, tables)
    }
}

pub struct Ledger<S> {
    services: S,
    saved: Option<(SavedSnapshot, String)>,
}
impl<S> Ledger<S> {
    pub fn new(services: S) -> Self {
        Self {
            services,
            saved: None,
        }
    }
    pub fn saved_snapshot(&self) -> Option<(&SavedSnapshot, &str)> {
        self.saved
            .as_ref()
            .map(|(receipt, logical)| (receipt, logical.as_str()))
    }
    pub fn services(&self) -> &S {
        &self.services
    }
    pub fn services_mut(&mut self) -> &mut S {
        &mut self.services
    }
}
impl<S> sealed::Sealed for Ledger<S> {}
impl<S: DeliveryServices> DeliveryServices for Ledger<S> {
    fn ledger_tag(&self) -> &str {
        self.services.ledger_tag()
    }
    fn save_forced(&mut self, tables: &Value) -> StageResult<SavedSnapshot> {
        let digest = digest(tables)?;
        let receipt = self.services.save_forced(tables)?;
        crate::validation::require(
            receipt.digest.len() == 64
                && receipt
                    .digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "Invalid saved snapshot digest",
        )?;
        self.saved = Some((receipt.clone(), digest));
        Ok(receipt)
    }
    fn save(&mut self, tables: &Value) -> StageResult<()> {
        self.services.save(tables)
    }
    fn save_with_force(&mut self, tables: &Value, force: bool) -> StageResult<()> {
        if force {
            self.save_forced(tables)?;
        } else {
            self.services.save_with_force(tables, false)?;
        }
        Ok(())
    }
    fn source(&mut self, descriptor: &Value) -> StageResult<SourceFile> {
        self.services.source(descriptor)
    }
    fn allow(&mut self, lane: &str, key: &str) -> bool {
        self.services.allow(lane, key)
    }
    fn queue_public_link(&mut self, intent: Persisted<LinkIntent>) -> StageResult<Value> {
        self.services.queue_public_link(intent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mathesis_delivery::StageError;
    use crate::{object, string};
    struct Saver {
        fail: bool,
    }
    impl DeliveryServices for Saver {
        fn ledger_tag(&self) -> &str {
            "synthetic"
        }
        fn save_forced(&mut self, tables: &Value) -> StageResult<SavedSnapshot> {
            if self.fail {
                Err(StageError {
                    summary: object([("type", string("TimeoutError"))]),
                })
            } else {
                Ok(SavedSnapshot {
                    generation: 7,
                    digest: outreach_canon::digest(tables)?,
                })
            }
        }
        fn save(&mut self, _: &Value) -> StageResult<()> {
            Ok(())
        }
        fn source(&mut self, _: &Value) -> StageResult<SourceFile> {
            unreachable!()
        }
        fn allow(&mut self, _: &str, _: &str) -> bool {
            true
        }
        fn queue_public_link(&mut self, _: Persisted<LinkIntent>) -> StageResult<Value> {
            unreachable!()
        }
    }
    #[test]
    fn acknowledged_snapshot_is_bound_and_changed_state_is_refused() {
        let mut ledger = Ledger::new(Saver { fail: false });
        let snapshot = object([("intent", string("synthetic-write"))]);
        assert!(Persisted::saved((), &ledger, &snapshot).is_err());
        let permit = Persisted::save((), &mut ledger, &snapshot).expect("save acknowledged");
        assert_eq!(permit.generation(), 7);
        assert_eq!(
            permit.snapshot_digest(),
            digest(&snapshot).expect("canonical snapshot")
        );
        let changed = object([("intent", string("changed-write"))]);
        assert!(Persisted::saved((), &ledger, &changed).is_err());
        ledger.services_mut().fail = true;
        assert!(Persisted::save((), &mut ledger, &changed).is_err());
        assert_eq!(
            ledger.saved_snapshot().expect("previous snapshot").0.digest,
            permit.snapshot_digest()
        );
    }
}
