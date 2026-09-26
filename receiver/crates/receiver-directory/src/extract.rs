//! Authenticated recovery with the public all-zero OVK. This never uses wallet keys.
use crate::{Error, Hash, Payment, Receiver};
use orchard::{
    keys::OutgoingViewingKey,
    note::{ExtractedNoteCommitment, Nullifier},
    note_encryption::{CompactAction, IronwoodDomain},
    value::ValueCommitment,
};
use zcash_note_encryption::{try_output_recovery_with_ovk, EphemeralKeyBytes, ShieldedOutput};

pub struct Action {
    pub cv: Hash,
    pub nullifier: Hash,
    pub cmx: Hash,
    pub ephemeral_key: Hash,
    pub enc_ciphertext: [u8; 580],
    pub out_ciphertext: [u8; 80],
}

impl ShieldedOutput<IronwoodDomain, 580> for Action {
    fn ephemeral_key(&self) -> EphemeralKeyBytes {
        EphemeralKeyBytes(self.ephemeral_key)
    }
    fn cmstar_bytes(&self) -> Hash {
        self.cmx
    }
    fn enc_ciphertext(&self) -> &[u8; 580] {
        &self.enc_ciphertext
    }
}

impl Action {
    /// Join compact directory data with an enhancement record. This does not authenticate
    /// the note or its chain position. The caller must decrypt and validate before crediting it.
    pub fn from_payment(
        payment: &Payment,
        enc_ciphertext_suffix: &[u8; 528],
        cv: Hash,
        out_ciphertext: [u8; 80],
    ) -> Self {
        let mut enc_ciphertext = [0; 580];
        enc_ciphertext[..52].copy_from_slice(&payment.ciphertext_prefix);
        enc_ciphertext[52..].copy_from_slice(enc_ciphertext_suffix);
        Self {
            cv,
            nullifier: payment.action_nullifier,
            cmx: payment.cmx,
            ephemeral_key: payment.ephemeral_key,
            enc_ciphertext,
            out_ciphertext,
        }
    }

    /// None means outside this recovery scheme, not an unused wallet address.
    pub fn recover_receiver(&self) -> Result<Option<Receiver>, Error> {
        let nf = Option::from(Nullifier::from_bytes(&self.nullifier)).ok_or(Error::Malformed)?;
        let cmx =
            Option::from(ExtractedNoteCommitment::from_bytes(&self.cmx)).ok_or(Error::Malformed)?;
        let cv = Option::from(ValueCommitment::from_bytes(&self.cv)).ok_or(Error::Malformed)?;
        let compact = CompactAction::from_parts(
            nf,
            cmx,
            EphemeralKeyBytes(self.ephemeral_key),
            self.enc_ciphertext[..52].try_into().unwrap(),
        );
        try_output_recovery_with_ovk(
            &IronwoodDomain::for_compact_action(&compact),
            &OutgoingViewingKey::from([0; 32]),
            self,
            &cv,
            &self.out_ciphertext,
        )
        .map(|(_, address, _)| Receiver::from_bytes(address.to_raw_address_bytes()))
        .transpose()
    }
}
