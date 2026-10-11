//! Excludes ephemeral thread scopes from retained diagnostics.
//!
//! Thread owners mark their spans with `ephemeral = true`. Spawned tasks must
//! carry that span (or create their own marked span). Privacy is inherited from
//! any ancestor and cannot be cleared by a child or a later field update.

use tracing::Event;
use tracing::Metadata;
use tracing::Subscriber;
use tracing::field::Field;
use tracing::field::Visit;
use tracing::span::Attributes;
use tracing::span::Id;
use tracing::span::Record;
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::Filter;
use tracing_subscriber::registry::LookupSpan;

/// A per-layer filter for feedback logs and metadata, independent of SQLite.
#[derive(Clone, Copy, Debug, Default)]
pub struct EphemeralThreadFilter;

impl<S> Filter<S> for EphemeralThreadFilter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, _metadata: &Metadata<'_>, _ctx: &Context<'_, S>) -> bool {
        // Keep spans observable so their privacy marker reaches descendants.
        true
    }

    fn event_enabled(&self, event: &Event<'_>, ctx: &Context<'_, S>) -> bool {
        !is_ephemeral_event(event, ctx)
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        record_ephemeral(attrs, id, &ctx);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        record_ephemeral(values, id, &ctx);
    }
}

#[derive(Default)]
struct Ephemeral(bool);

impl Visit for Ephemeral {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "ephemeral" {
            self.0 |= value;
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

pub(crate) fn record_ephemeral<S>(fields: impl RecordFields, id: &Id, ctx: &Context<'_, S>)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let mut marker = Ephemeral::default();
    fields.record(&mut marker);
    if marker.0
        && let Some(span) = ctx.span(id)
    {
        // Multiple retained-log layers may observe the same span.
        span.extensions_mut().replace(marker);
    }
}

pub(crate) fn is_ephemeral_event<S>(event: &Event<'_>, ctx: &Context<'_, S>) -> bool
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let marked = |span: tracing_subscriber::registry::SpanRef<'_, S>| {
        span.extensions()
            .get::<Ephemeral>()
            .is_some_and(|marker| marker.0)
    };
    // An explicit parent must not let code inside a private scope escape it.
    ctx.event_scope(event)
        .is_some_and(|mut scope| scope.any(&marked))
        || ctx
            .lookup_current()
            .is_some_and(|span| span.scope().any(marked))
}
