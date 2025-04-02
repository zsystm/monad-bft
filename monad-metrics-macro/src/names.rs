use syn::{
    bracketed,
    parse::{Parse, ParseStream},
    PathSegment, Token,
};

pub struct MetricGroup {
    names: Vec<PathSegment>,
}

impl Parse for MetricGroup {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let content;
        bracketed!(content in input);

        let mut names = Vec::default();

        while !content.is_empty() {
            names.push(content.parse()?);

            if content.is_empty() {
                break;
            }

            content.parse::<Token![,]>()?;
        }

        Ok(Self { names })
    }
}

impl MetricGroup {
    pub fn into_vec(self) -> Vec<PathSegment> {
        self.names
    }
}
