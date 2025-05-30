use convert_case::{Case, Casing};
use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::TokenStreamExt;
use syn::{
    braced,
    parse::{discouraged::Speculative, Parse, ParseBuffer, ParseStream},
    spanned::Spanned,
    Ident, Result, Token,
};

pub use self::{
    attrs::{Attrs, HistogramAttrs, ScalarAttrs, ScalarLabelAttrs},
    name::MetricName,
};

mod attrs;
mod name;

pub struct MetricsInput {
    pub namespace: Ident,
    pub struct_name: Ident,
    pub ts_error: TokenStream,
    pub scalars: Vec<MetricName<Attrs<ScalarAttrs>>>,
    pub histograms: Vec<MetricName<Attrs<HistogramAttrs>>>,
    pub components: Vec<MetricsInput>,
}

impl Parse for MetricsInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let namespace_attrs: Option<Attrs<Ident>> = input.parse().ok();

        let struct_name = input.parse::<Ident>()?;

        let namespace = namespace_attrs
            .map(|Attrs { data }| data)
            .unwrap_or_else(|| {
                Ident::new(
                    &struct_name.to_string().to_case(Case::Snake),
                    struct_name.span(),
                )
            });

        let mut ts_error = TokenStream::new();

        if ["counters", "gauges", "histograms"].contains(&namespace.to_string().as_str()) {
            ts_error.append_all([quote::quote_spanned! {
                namespace.span() => compile_error!("Metric namespace cannot be instrument name!");
            }]);
        }

        let content;
        braced!(content in input);

        let mut scalars: Vec<MetricName<Attrs<ScalarAttrs>>> = Vec::default();
        let mut histograms: Vec<MetricName<Attrs<HistogramAttrs>>> = Vec::default();
        let mut components: Vec<MetricsInput> = Vec::default();

        loop {
            if let Ok(scalar) = fork_parse(&content) {
                scalars.push(scalar);
            } else if let Ok(histogram) = fork_parse(&content) {
                histograms.push(histogram);
            } else if let Ok(component) = fork_parse(&content) {
                components.push(component);
            } else {
                break;
            }

            if content.is_empty() {
                break;
            }

            content.parse::<Token![,]>()?;
        }

        // for metric in scalars.iter_mut() {
        //     if metric.path.ident.to_string().ends_with("_total") {
        //         metric.ts_error.append_all([quote::quote_spanned! {
        //             metric.path.span() => compile_error!("Metric should not end in _total");
        //         }]);
        //     }
        // }

        // for (ident, ts_error) in scalars
        //     .iter_mut()
        //     .map(|metric| (metric.path.ident.to_owned(), &mut metric.ts_error))
        //     .duplicates_by(|(ident, _)| ident.to_string())
        // {
        //     ts_error.append_all([quote::quote_spanned! {
        //         path.span() => compile_error!("Duplicate metric name!");
        //     }]);
        // }

        // let components = content.parse().unwrap();

        if !content.is_empty() {
            return Err(syn::Error::new(content.span(), "Unexpected token stream!"));
        }

        Ok(Self {
            struct_name,
            namespace,
            ts_error,
            scalars,
            histograms,
            components,
        })
    }
}

fn fork_parse<T>(input: &ParseBuffer<'_>) -> Result<T>
where
    T: Parse,
{
    let fork = input.fork();

    let value = fork.parse()?;
    input.advance_to(&fork);

    Ok(value)
}

// const INVALID_METRIC_SUFFIXES: &'static [&'static str] = &["_total", "_bucket", "_count"];

// fn enforce_naming_suffixes<T>(metrics: &mut MetricNames<T>) {
//     for (path, ts_error) in metrics.iter_path_ts_error() {
//         for invalid_suffix in INVALID_METRIC_SUFFIXES {
//             if path.ident.to_string().ends_with(invalid_suffix) {
//                 ts_error.append_all([quote::quote_spanned! {
//                     path.span() => compile_error!(format!("Metric should not end in {}", invalid_suffix));
//                 }]);
//             }
//         }
//     }
// }
