//! Codec-selection policy.
//!
//! `oxideav-core::CodecRegistry` is registration-only plus a couple of
//! "first-match" lookups (`first_decoder` / `decoder_by_impl` and
//! their encoder counterparts) for single-impl test scenarios. It
//! does NOT bias the choice across multiple candidates — that policy
//! lives here.
//!
//! Free functions [`make_decoder`] / [`make_decoder_with`] /
//! [`make_encoder`] / [`make_encoder_with`] take `&CodecRegistry` as
//! the first argument. Each walks every implementation registered for
//! `params.codec_id` in increasing priority order, filters by the
//! impl's `caps.fits_params` restrictions and the preferences'
//! exclude rules, then tries each factory until one returns `Ok`.
//!
//! Init-time fallback ("hardware session creation failed → try the SW
//! path") is implemented here: on `Err` from the highest-priority
//! factory, the next candidate is attempted; the last error is
//! surfaced if every candidate fails. To opt out of the SW fallback
//! when the pipeline truly needs hardware, set
//! `CodecPreferences { require_hardware: true, .. }`.

use oxideav_core::{
    CodecCapabilities, CodecImplementation, CodecParameters, CodecRegistry, Decoder, Encoder,
    Error, Result,
};

/// User preferences for codec selection — pass to `make_decoder_with`
/// / `make_encoder_with` (free functions or via the
/// [`CodecRegistryExt`] trait) to bias the choice.
#[derive(Clone, Debug, Default)]
pub struct CodecPreferences {
    /// Implementation names to prefer (boost their priority by `boost`).
    pub prefer: Vec<String>,
    /// Implementation names to skip entirely.
    pub exclude: Vec<String>,
    /// Forbid hardware-accelerated impls. Mutually exclusive with
    /// `require_hardware` — if both are set, no impl is selectable.
    pub no_hardware: bool,
    /// Forbid software-only impls — only hardware-accelerated factories
    /// are considered. The init-time fallback (try next priority on
    /// factory `Err`) still applies *within* the HW candidate set, but
    /// it will NOT silently degrade to the SW path if every HW factory
    /// fails. Use this when the pipeline needs hardware (real-time
    /// low-latency capture, energy budget, etc.) — the resulting
    /// `make_decoder_with` / `make_encoder_with` will surface the
    /// underlying `OSStatus` / device error.
    pub require_hardware: bool,
    /// Boost amount for `prefer` impls (subtracted from priority).
    pub boost: i32,
}

impl CodecPreferences {
    pub fn excludes(&self, caps: &CodecCapabilities) -> bool {
        self.exclude.iter().any(|n| n == &caps.implementation)
            || (self.no_hardware && caps.hardware_accelerated)
            || (self.require_hardware && !caps.hardware_accelerated)
    }

    pub fn effective_priority(&self, caps: &CodecCapabilities) -> i32 {
        if self.prefer.iter().any(|n| n == &caps.implementation) {
            caps.priority - self.boost.max(0)
        } else {
            caps.priority
        }
    }
}

/// Free-function shorthand: `make_decoder(reg, params)` with default
/// preferences. Equivalent to
/// `make_decoder_with(reg, params, &CodecPreferences::default())`.
pub fn make_decoder(reg: &CodecRegistry, params: &CodecParameters) -> Result<Box<dyn Decoder>> {
    make_decoder_with(reg, params, &CodecPreferences::default())
}

/// Build a decoder for `params` honouring `prefs`. Walks every
/// implementation registered for `params.codec_id` in increasing
/// effective-priority order, skipping any excluded by the prefs, then
/// tries each factory until one returns `Ok`. The last error is
/// surfaced if every candidate fails.
pub fn make_decoder_with(
    reg: &CodecRegistry,
    params: &CodecParameters,
    prefs: &CodecPreferences,
) -> Result<Box<dyn Decoder>> {
    make_decoder_with_selection(reg, params, prefs).map(|(decoder, _)| decoder)
}

/// Same selection walk as [`make_decoder_with`], but also returns the
/// capabilities record for the implementation whose factory actually
/// succeeded. This is intended for runtime diagnostics: callers can report
/// the real implementation after preference filtering and init-time fallback
/// instead of inferring it from the requested policy.
pub fn make_decoder_with_selection(
    reg: &CodecRegistry,
    params: &CodecParameters,
    prefs: &CodecPreferences,
) -> Result<(Box<dyn Decoder>, CodecCapabilities)> {
    let candidates = reg.implementations(&params.codec_id);
    if candidates.is_empty() {
        return Err(Error::CodecNotFound(params.codec_id.to_string()));
    }
    let mut ranked: Vec<&CodecImplementation> = candidates
        .iter()
        .filter(|i| i.make_decoder.is_some() && !prefs.excludes(&i.caps))
        .filter(|i| i.caps.fits_params(params, false))
        .collect();
    ranked.sort_by_key(|i| prefs.effective_priority(&i.caps));
    let mut last_err: Option<Error> = None;
    for imp in ranked {
        match (imp.make_decoder.unwrap())(params) {
            Ok(decoder) => return Ok((decoder, imp.caps.clone())),
            Err(error) => last_err = Some(error),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        Error::CodecNotFound(format!(
            "no decoder for {} accepts the requested parameters",
            params.codec_id
        ))
    }))
}

/// Free-function shorthand for encoder construction with default prefs.
pub fn make_encoder(reg: &CodecRegistry, params: &CodecParameters) -> Result<Box<dyn Encoder>> {
    make_encoder_with(reg, params, &CodecPreferences::default())
}

/// Build an encoder for `params` honouring `prefs`. Same shape as
/// [`make_decoder_with`].
pub fn make_encoder_with(
    reg: &CodecRegistry,
    params: &CodecParameters,
    prefs: &CodecPreferences,
) -> Result<Box<dyn Encoder>> {
    make_encoder_with_selection(reg, params, prefs).map(|(encoder, _)| encoder)
}

/// Same selection walk as [`make_encoder_with`], but also returns the
/// capabilities record for the implementation whose factory actually
/// succeeded.
pub fn make_encoder_with_selection(
    reg: &CodecRegistry,
    params: &CodecParameters,
    prefs: &CodecPreferences,
) -> Result<(Box<dyn Encoder>, CodecCapabilities)> {
    let candidates = reg.implementations(&params.codec_id);
    if candidates.is_empty() {
        return Err(Error::CodecNotFound(params.codec_id.to_string()));
    }
    let mut ranked: Vec<&CodecImplementation> = candidates
        .iter()
        .filter(|i| i.make_encoder.is_some() && !prefs.excludes(&i.caps))
        .filter(|i| i.caps.fits_params(params, true))
        .collect();
    ranked.sort_by_key(|i| prefs.effective_priority(&i.caps));
    let mut last_err: Option<Error> = None;
    for imp in ranked {
        match (imp.make_encoder.unwrap())(params) {
            Ok(encoder) => return Ok((encoder, imp.caps.clone())),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        Error::CodecNotFound(format!(
            "no encoder for {} accepts the requested parameters",
            params.codec_id
        ))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps(name: &str, hw: bool) -> CodecCapabilities {
        CodecCapabilities::audio(name).with_hardware(hw)
    }

    #[test]
    fn no_hardware_excludes_hw_only() {
        let prefs = CodecPreferences {
            no_hardware: true,
            ..Default::default()
        };
        assert!(prefs.excludes(&caps("aac_audiotoolbox", true)));
        assert!(!prefs.excludes(&caps("aac_sw", false)));
    }

    #[test]
    fn require_hardware_excludes_sw_only() {
        let prefs = CodecPreferences {
            require_hardware: true,
            ..Default::default()
        };
        assert!(!prefs.excludes(&caps("aac_audiotoolbox", true)));
        assert!(prefs.excludes(&caps("aac_sw", false)));
    }

    #[test]
    fn no_hardware_and_require_hardware_excludes_everything() {
        let prefs = CodecPreferences {
            no_hardware: true,
            require_hardware: true,
            ..Default::default()
        };
        assert!(prefs.excludes(&caps("aac_audiotoolbox", true)));
        assert!(prefs.excludes(&caps("aac_sw", false)));
    }

    struct SelectedDecoder {
        codec_id: oxideav_core::CodecId,
    }

    impl Decoder for SelectedDecoder {
        fn codec_id(&self) -> &oxideav_core::CodecId {
            &self.codec_id
        }

        fn send_packet(&mut self, _packet: &oxideav_core::Packet) -> Result<()> {
            Ok(())
        }

        fn receive_frame(&mut self) -> Result<oxideav_core::Frame> {
            Err(Error::NeedMore)
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn failing_decoder(_params: &CodecParameters) -> Result<Box<dyn Decoder>> {
        Err(Error::unsupported("preferred implementation unavailable"))
    }

    fn fallback_decoder(params: &CodecParameters) -> Result<Box<dyn Decoder>> {
        Ok(Box::new(SelectedDecoder {
            codec_id: params.codec_id.clone(),
        }))
    }

    #[test]
    fn selected_decoder_metadata_tracks_factory_that_actually_succeeds() {
        let codec_id = oxideav_core::CodecId::new("selection_metadata_test");
        let mut reg = CodecRegistry::new();
        reg.register(
            oxideav_core::CodecInfo::new(codec_id.clone())
                .capabilities(
                    CodecCapabilities::video("preferred_hw")
                        .with_hardware(true)
                        .with_priority(10),
                )
                .decoder(failing_decoder),
        );
        reg.register(
            oxideav_core::CodecInfo::new(codec_id.clone())
                .capabilities(
                    CodecCapabilities::video("fallback_sw")
                        .with_hardware(false)
                        .with_priority(100),
                )
                .decoder(fallback_decoder),
        );

        let params = CodecParameters::video(codec_id);
        let (_decoder, selected) =
            make_decoder_with_selection(&reg, &params, &CodecPreferences::default()).unwrap();

        assert_eq!(selected.implementation, "fallback_sw");
        assert!(!selected.hardware_accelerated);
    }
}
