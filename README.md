# Djanco

## DSL

Entry points:

- `projects`
- `commits`
- `snapshots`
- `users`
- `paths`

Verbs:

- `filter_by_attrib(Filter)`
- `select_attrib(Attribute)`
- `sort_by_attrib(Attribute)`
- `group_by_attrib(Attribute)`
- `ungroup`
- `sample(Sampler)`

Samplers:

- `sample::Top(usize)`
- `sample::Random(usize, Seed(u128))`
- `sample::Distinct(Sampler, SimilarityCritertion)`

Similerity criteria:

- `sample::Ratio(Attribute, f64)`

Statistical functions:

- 

Attributes: